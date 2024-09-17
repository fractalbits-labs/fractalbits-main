// use std::convert::TryFrom;
// use std::net::{SocketAddr, ToSocketAddrs};

// use anyhow::{anyhow, Result};
// use tokio::task::spawn_blocking;

// #[derive(Clone)]
// pub(crate) struct UserInput {
//     pub(crate) addr: SocketAddr,
//     pub(crate) host: String,
//     pub(crate) uri: Uri,
// }

// impl UserInput {
//     pub(crate) async fn new(string: String) -> Result<Self> {
//         spawn_blocking(move || Self::blocking_new(string))
//             .await
//             .unwrap()
//     }

//     fn blocking_new(string: String) -> Result<Self> {
//         let uri = Uri::try_from(string)?;
//         let authority = uri
//             .authority()
//             .ok_or_else(|| anyhow!("host not present on uri"))?;
//         let host = authority.host().to_owned();
//         let port = authority
//             .port_u16()
//             .unwrap_or_else(|| scheme.default_port());
//         let host_header = HeaderValue::from_str(&host)?;

//         // Prefer ipv4.
//         let addr_iter = (host.as_str(), port).to_socket_addrs()?;
//         let mut last_addr = None;
//         for addr in addr_iter {
//             last_addr = Some(addr);
//             if addr.is_ipv4() {
//                 break;
//             }
//         }
//         let addr = last_addr.ok_or_else(|| anyhow!("hostname lookup failed"))?;

//         Ok(Self {
//             addr,
//             scheme,
//             host,
//             host_header,
//             uri,
//             method,
//             headers,
//             body,
//         })
//     }
// }
