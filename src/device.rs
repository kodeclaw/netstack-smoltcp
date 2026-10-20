use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::BytesMut;
use smoltcp::{
    phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken},
    time::Instant,
};
use tokio::sync::mpsc::{Permit, Receiver, Sender, channel};

use crate::packet::AnyIpPktFrame;

pub(super) struct VirtualDevice {
    in_buf_avail: Arc<AtomicBool>,
    in_buf: Receiver<AnyIpPktFrame>,
    out_buf: Sender<AnyIpPktFrame>,
    mtu: usize,
}

impl VirtualDevice {
    pub(super) fn new(
        iface_egress_tx: Sender<AnyIpPktFrame>,
        mtu: usize,
    ) -> (Self, Sender<AnyIpPktFrame>, Arc<AtomicBool>) {
        let iface_ingress_tx_avail = Arc::new(AtomicBool::new(false));
        // Use a bounded channel for backpressure. 1024 packets is reasonable buffering.
        let (iface_ingress_tx, iface_ingress_rx) = channel(1024);
        (
            Self {
                in_buf_avail: iface_ingress_tx_avail.clone(),
                in_buf: iface_ingress_rx,
                out_buf: iface_egress_tx,
                mtu,
            },
            iface_ingress_tx,
            iface_ingress_tx_avail,
        )
    }
}

impl Device for VirtualDevice {
    type RxToken<'a> = VirtualRxToken;
    type TxToken<'a> = VirtualTxToken<'a>;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        // Reserve the response slot before dequeuing input. Otherwise, a full
        // egress queue would permanently discard a received packet.
        let Ok(permit) = self.out_buf.try_reserve() else {
            self.in_buf_avail.store(false, Ordering::Release);
            return None;
        };

        let Ok(buffer) = self.in_buf.try_recv() else {
            self.in_buf_avail.store(false, Ordering::Release);
            return None;
        };

        Some((Self::RxToken { buffer }, Self::TxToken { permit }))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        match self.out_buf.try_reserve() {
            Ok(permit) => Some(Self::TxToken { permit }),
            Err(_) => None,
        }
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ip;
        capabilities.max_transmission_unit = self.mtu;
        capabilities
    }
}

pub(super) struct VirtualRxToken {
    buffer: AnyIpPktFrame,
}

impl RxToken for VirtualRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buffer[..])
    }
}

pub(super) struct VirtualTxToken<'a> {
    permit: Permit<'a, AnyIpPktFrame>,
}

impl<'a> TxToken for VirtualTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buffer = BytesMut::zeroed(len);
        let result = f(&mut buffer);
        self.permit.send(buffer.freeze());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_egress_queue_does_not_discard_ingress_packet() {
        let (egress_tx, mut egress_rx) = channel(1);
        egress_tx
            .try_send(BytesMut::from(&b"egress"[..]).freeze())
            .unwrap();
        let (mut device, ingress_tx, _) = VirtualDevice::new(egress_tx, 1500);
        ingress_tx
            .try_send(BytesMut::from(&b"ingress"[..]).freeze())
            .unwrap();

        assert!(device.receive(Instant::now()).is_none());
        assert_eq!(&egress_rx.try_recv().unwrap()[..], b"egress");

        let (rx, _tx) = device
            .receive(Instant::now())
            .expect("the packet must still be queued after egress backpressure");
        assert_eq!(rx.consume(|packet| packet.to_vec()), b"ingress");
    }
}
