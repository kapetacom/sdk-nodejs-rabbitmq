import {AMQPError, Connection} from "rabbitmq-client"
import {ConfigProvider, InstanceOperator} from "@kapeta/sdk-config";
import {OperatorOptions, RabbitMQExchangeResource, RabbitMQQueueResource} from "./types";
import {ConnectionOptions} from "rabbitmq-client/lib/normalize";
import {Cmd, MethodParams} from "rabbitmq-client/lib/codec";

export async function createVHost(managementOperator:InstanceOperator, instanceId:string) {

    const port = managementOperator.ports['management']?.port || 15672;
    const rabbitMQServer = `http://${managementOperator.hostname}:${port}`; // Replace with your RabbitMQ server URL
    const username = managementOperator.credentials?.username;
    const password = managementOperator.credentials?.password;
    const vhostName = instanceId;

    const url = `${rabbitMQServer}/api/vhosts/${encodeURIComponent(vhostName)}`;

    const options = {
        method: 'PUT',
        headers: {
            'Authorization': 'Basic ' + Buffer.from(username + ':' + password).toString('base64'),
            'Content-Type': 'application/json'
        }
    };

    console.log(`Ensuring RabbitMQ vhost: ${vhostName} @ ${managementOperator.hostname}:${port}`);

    const response = await fetch(url, options);
    if (!response.ok) {
        throw new Error(`Failed to create vhost: ${response.status} : ${response.statusText}`);
    }
    return vhostName;
}

export async function connectToInstance(config:ConfigProvider, instanceId:string) {
    const operator = await config.getInstanceOperator<OperatorOptions>(instanceId);
    if (!operator) {
        throw new Error(`No operator found for instance ${instanceId}`);
    }
    const vhost = await createVHost(operator, instanceId);

    const port = operator.ports['amqp']?.port || 5672;

    const connectOptions:ConnectionOptions = {
        hostname: operator.hostname,
        port,
        username: operator.credentials?.username,
        password: operator.credentials?.password,
        vhost,
    }

    console.log(`Connecting to RabbitMQ on ${connectOptions.hostname}:${connectOptions.port} as ${connectOptions.username}@${connectOptions.vhost}`)

    const connection = new Connection(connectOptions);

    connection.on('error', (err) => {
        console.log('RabbitMQ connection error', err)
    });

    connection.on('connection', () => {
        console.log('Connection successfully (re)established')
    });

    // Test connection
    const testChannel = await connection.acquire();
    await testChannel.close()

    return connection;
}

export function asExchange(exchange:RabbitMQExchangeResource):MethodParams[Cmd.ExchangeDeclare] {
    return {
        exchange: exchange.metadata.name,
        durable: exchange.spec.durable,
        autoDelete: exchange.spec.autoDelete,
        type: exchange.spec.exchangeType,
    }
}

export function asQueue(queue:RabbitMQQueueResource):MethodParams[Cmd.QueueDeclare] {
    const queueRequestName = queue.spec.exclusive ? '' : queue.metadata.name;

    return {
        durable: queue.spec.durable,
        autoDelete: queue.spec.autoDelete,
        queue: queueRequestName,
        exclusive: queue.spec.exclusive,
    };
}


export async function exchangeEnsure(connection:Connection, exchange: MethodParams[Cmd.ExchangeDeclare]) {
    try {
        await connection.exchangeDeclare(exchange)
    } catch (e:any) {
        const amqpError = e as AMQPError;
        if (amqpError.code === 'PRECONDITION_FAILED') {
            // Exchange exists with different parameters
            console.warn(`Exchange ${exchange.exchange} already exists with different parameters: %s.`, e.message)

            console.warn(`Recreating exchange ${exchange.exchange}`);
            await connection.exchangeDelete({
                exchange: exchange.exchange,
            });
            await connection.exchangeDeclare(exchange);
            return;
        }
        throw e;
    }
}

export async function exchangeBindingEnsure(connection:Connection, exchangeBinding: MethodParams[Cmd.ExchangeBind]) {
    await connection.exchangeBind(exchangeBinding);
}

export async function queueEnsure(connection:Connection, queue: MethodParams[Cmd.QueueDeclare]) {
    try {
        await connection.queueDeclare(queue)
    } catch (e:any) {
        const amqpError = e as AMQPError;
        if (amqpError.code === 'PRECONDITION_FAILED') {
            // Queue exists with different parameters
            console.warn(`Queue ${queue.queue} already exists with different parameters: %s`, e.message)

            console.warn(`Recreating Queue ${queue.queue}`);
            try {
                await connection.queueDelete({
                    ifEmpty: true,
                    queue: queue.queue,
                });
                await connection.queueDeclare(queue);
            } catch (e) {
                console.warn(`Failed to recreate queue ${queue.queue}`, e);
            }
            return;
        }
        throw e;
    }
}

export async function queueBindingEnsure(connection:Connection, queueBinding: MethodParams[Cmd.QueueBind]) {
    await connection.queueBind(queueBinding);
}