# Live Stock Price & Financial Risk Terminal
This is an end-to-end, event-driven data streaming pipeline designed to process real-time financial market data, compute risk metrics,
detect anomalies, and serve dynamic updates to an interactive user interface. Built with a containerized microservices approach orchestrated via Docker, 
the platform utilizes Apache Kafka to ingest live stock feeds continuously from external market gateways, processing streams asynchronously through 
dedicated producer and consumer nodes to deliver low-latency telemetry directly to a financial risk visualization dashboard.


## Execute in powershell, docker-compose up --build -d
## access the dashboard on the browser,
  http://localhost:8501/

  You can find the ** architectural document ** and the test result in the Artifacts folder
