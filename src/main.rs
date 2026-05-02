use kafka_lite::server::TcpServer;
use kafka_lite::store::broker::Broker;
use anyhow::Result;

fn main() -> Result<()>{
    env_logger::init();
    let broker = Broker::try_new("data")?;
    let server = TcpServer::try_new("localhost:5500", broker)?;
    server.listen();
    Ok(())
}
