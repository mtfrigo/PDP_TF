# Instalação

## Mosquitto

```
sudo apt−get install mosquitto mosquitto−clients
```

## RabbitMQ (com Docker)

```
docker run -it –rm –name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

# Rodando o Benchmark

Primeiramente, é necessário alterar as variáveis da execução no arquivo client.py do respectivo Message Broker (na pasta mosquitto ou rabbitmq).
As variáveis alteradas para os testes são: 

* MESSAGE_INTERVAL: Intervalo de transferência das mensagens de cada cliente (em segundos)
* PUBLISHERS: Quantidade de clientes
* BENCHMARK_DURATION: Duração do benchmark
* WARMUP_DURATION: Duração do aquecimento
* BROKER_IP: IP do broker 

Depois de escolhido os valores das variáveis acima, deve-se executar a aplicação server.py na instância desejada (cujo IP foi específica acima).

Quando o servidor estiver inicializado haverá uma mensagem "Waiting for command to start..", então a aplicação client.py pode ser executada na instância desejada.

No final a execução, ambas instância (servidor e cliente) terão gerado os respectivos arquivos .csv com os valores medidos.
