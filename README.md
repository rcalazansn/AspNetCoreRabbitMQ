# AspNetCoreRabbitMQ

## Passo 1: Producer.Worker
Enviar mensagem para o rabbit de forma aleatoria

## Passo 2: Producer.Worker 
Consome as mensagem da fila principal,  quando o conteúdo da mensagem é igual "retry", é enviado para a fila "retry_queue"

## Passo 3: Retry.Worker 
Consome a fila "retry_queue", caso a mensma mensage seja consumida por 3x é enviado para a fila "deadletter"
