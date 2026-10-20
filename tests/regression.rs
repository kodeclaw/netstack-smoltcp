//! Regression tests that reproduce the bugs found in the static analysis.

use std::time::Duration;

use bytes::Bytes;
use etherparse::{IpNumber, Ipv4Header, PacketBuilder, UdpHeader};
use futures::{SinkExt, StreamExt};
use tokio::time::timeout;

use netstack_smoltcp::StackBuilder;

fn make_udp_ipv4(
    src_ip: [u8; 4],
    src_port: u16,
    dst_ip: [u8; 4],
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let udp_hdr = UdpHeader::with_ipv4_checksum(
        src_port,
        dst_port,
        &Ipv4Header::new(
            (UdpHeader::LEN + payload.len()) as u16,
            64,
            IpNumber::UDP,
            src_ip,
            dst_ip,
        )
        .unwrap(),
        payload,
    )
    .unwrap();

    let ip_hdr = Ipv4Header::new(
        (UdpHeader::LEN + payload.len()) as u16,
        64,
        IpNumber::UDP,
        src_ip,
        dst_ip,
    )
    .unwrap();

    let mut buf = Vec::with_capacity(Ipv4Header::MIN_LEN + UdpHeader::LEN + payload.len());
    ip_hdr.write(&mut buf).unwrap();
    udp_hdr.write(&mut buf).unwrap();
    buf.extend_from_slice(payload);
    buf
}

/// before(include) a15e0b72bfc72cb032e67138070da01e325d66f8
/// sink_buf is used in `Stack` to hold a slot for sending any pkt
///
/// the original assumption is that the `poll_ready` -> `start_send` -> `poll_flush`
/// are called sequentially so the slot could be reused and will never get blocked.
///
/// but once the user calls `send_all` on `Stack`, which will not immediate flush the pkt(call `poll_flush`),
/// then `sink_buf` is could be Some(pkt), then it will trigger `Poll::Pending` branch in `Stack::poll_ready`,
/// who did not register the waker correctly, so it will got hanged forever.
#[tokio::test(flavor = "current_thread")]
async fn bug1_poll_ready_waker_registered_via_send_all() {
    let (mut stack, _runner, _udp_socket, _tcp) = StackBuilder::default()
        .enable_udp(true)
        .udp_buffer_size(64)
        .stack_buffer_size(64)
        .build()
        .unwrap();

    let pkt1 = make_udp_ipv4([1, 2, 3, 4], 1111, [5, 6, 7, 8], 9999, b"first");
    let pkt2 = make_udp_ipv4([1, 2, 3, 4], 1111, [5, 6, 7, 8], 9999, b"second");

    let mut stream =
        futures::stream::iter([Ok(bytes::Bytes::from(pkt1)), Ok(bytes::Bytes::from(pkt2))]);

    let result = timeout(Duration::from_secs(1), stack.send_all(&mut stream)).await;
    // should be ok after the fix
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn udp_zero_length_datagram_is_forwarded() {
    let (mut stack, _runner, udp_socket, _tcp) =
        StackBuilder::default().enable_udp(true).build().unwrap();
    let (_reader, mut writer) = udp_socket.unwrap().split();
    let source = "192.0.2.1:12345".parse().unwrap();
    let destination = "198.51.100.2:53".parse().unwrap();

    writer
        .send((Bytes::new(), source, destination))
        .await
        .unwrap();

    let frame = timeout(Duration::from_secs(1), stack.next())
        .await
        .expect("a zero-length UDP datagram must be emitted")
        .expect("stack should remain open")
        .unwrap();
    assert_eq!(frame[0] >> 4, 4);
    assert_eq!(frame.len(), 28);
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_udp_datagram_does_not_terminate_receive_stream() {
    let (mut stack, _runner, udp_socket, _tcp) =
        StackBuilder::default().enable_udp(true).build().unwrap();
    let (mut reader, _writer) = udp_socket.unwrap().split();

    let mut malformed = Vec::new();
    Ipv4Header::new(3, 64, IpNumber::UDP, [192, 0, 2, 1], [198, 51, 100, 2])
        .unwrap()
        .write(&mut malformed)
        .unwrap();
    malformed.extend_from_slice(&[0, 1, 2]);
    let valid = make_udp_ipv4([192, 0, 2, 1], 12345, [198, 51, 100, 2], 53, b"valid");

    stack.send(Bytes::from(malformed)).await.unwrap();
    stack.send(Bytes::from(valid)).await.unwrap();

    let (payload, _, _) = timeout(Duration::from_secs(1), reader.next())
        .await
        .expect("receiver should recover from the invalid packet")
        .expect("one malformed frame must not close the UDP stream");
    assert_eq!(payload, Bytes::from_static(b"valid"));
}

#[tokio::test(flavor = "current_thread")]
async fn closing_stack_sink_flushes_buffered_packet() {
    let (mut stack, _runner, udp_socket, _tcp) =
        StackBuilder::default().enable_udp(true).build().unwrap();
    let (mut reader, _writer) = udp_socket.unwrap().split();
    let packet = make_udp_ipv4([192, 0, 2, 1], 12345, [198, 51, 100, 2], 53, b"last");

    stack.feed(Bytes::from(packet)).await.unwrap();
    stack.close().await.unwrap();

    let (payload, _, _) = timeout(Duration::from_secs(1), reader.next())
        .await
        .expect("closing a Sink should deliver the last buffered frame")
        .expect("UDP receiver should still be alive");
    assert_eq!(payload, Bytes::from_static(b"last"));
}

#[test]
fn zero_channel_capacity_returns_an_error_instead_of_panicking() {
    assert!(
        StackBuilder::default()
            .stack_buffer_size(0)
            .build()
            .is_err()
    );
    assert!(
        StackBuilder::default()
            .enable_udp(true)
            .udp_buffer_size(0)
            .build()
            .is_err()
    );
    assert!(
        StackBuilder::default()
            .enable_tcp(true)
            .tcp_buffer_size(0)
            .build()
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn retransmitted_tcp_syn_does_not_create_duplicate_stream() {
    let (mut stack, runner, _udp, listener) =
        StackBuilder::default().enable_tcp(true).build().unwrap();
    let _runner = tokio::spawn(runner.unwrap());
    let mut listener = listener.unwrap();

    let builder = PacketBuilder::ipv4([192, 0, 2, 1], [198, 51, 100, 2], 64)
        .tcp(12345, 443, 1, 65535)
        .syn();
    let mut syn = Vec::with_capacity(builder.size(0));
    builder.write(&mut syn, &[]).unwrap();
    let frame = Bytes::from(syn);

    stack.send(frame.clone()).await.unwrap();
    let (_stream, _local, _remote) = timeout(Duration::from_secs(1), listener.next())
        .await
        .expect("the first TCP SYN should create a stream")
        .expect("TCP listener must be open");

    stack.send(frame).await.unwrap();
    assert!(
        timeout(Duration::from_millis(200), listener.next())
            .await
            .is_err(),
        "retransmitting the same SYN must not create another TCP stream"
    );
}
