import {ConfigProvider} from "@kapeta/sdk-config";
import {RabbitMQBlockDefinition, RabbitMQQueueResource} from "./types";
import {asExchange, asQueue, connectToInstance, exchangeEnsure, queueBindingEnsure, queueEnsure} from "./shared";
import type {AsyncMessage, Cmd, Envelope, MethodParams} from "rabbitmq-client/lib/codec";
import {Consumer, ConsumerStatus} from "rabbitmq-client/lib/Consumer";

export interface ConsumerOptions {
    qos?: MethodParams[Cmd.BasicQos]
    requeue?: boolean,
    concurrency?: number,
}

export type TypedAsyncMessage<DataType> = AsyncMessage & { body: DataType }
export type MessageHandler<DataType = any> = (data:DataType, msg: TypedAsyncMessage<DataType>) => Promise<ConsumerStatus | void> | ConsumerStatus | void;

export const DROP_MESSAGE = new Error('Drop message');

export async function createConsumer<DataType = any>(config: ConfigProvider, resourceName: string, callback: MessageHandler<DataType>, opts?: ConsumerOptions): Promise<Consumer> {
    const instance = await config.getInstanceForConsumer(resourceName);
    if (!instance) {
        throw new Error(`Could not find instance for consumer ${resourceName}`);
    }
    const rabbitBlock = instance.block as RabbitMQBlockDefinition;

    if (!rabbitBlock.spec.consumers?.length ||
        !rabbitBlock.spec.providers?.length ||
        !rabbitBlock.spec.bindings?.exchanges?.length) {
        throw new Error(`Invalid rabbitmq block definition. Missing consumers, providers and/or bindings for instance ${instance.instanceId} of consumer ${resourceName}`);
    }

    const connection = await connectToInstance(config, instance.instanceId);

    // Get the defined exchanges that this publisher should publish to
    const blockResources = instance.connections.map((connection) => {
        return rabbitBlock.spec.providers?.find((provider) => {
            return provider.metadata.name === connection.provider.resourceName &&
                connection.consumer.resourceName === resourceName;
        });
    }).filter(Boolean) as RabbitMQQueueResource[];

    if (blockResources.length === 0) {
        throw new Error('No defined queues found');
    }

    if (blockResources.length > 1) {
        throw new Error('Multiple defined queues found. Only 1 expected');
    }

    const queue = blockResources[0];
    const queueOptions = asQueue(queue);

    // Bind exchanges to queue
    const exchanges: MethodParams[Cmd.ExchangeDeclare][] = [];
    const queueBindings: MethodParams[Cmd.QueueBind][] = [];
    for (const exchangeBindings of rabbitBlock.spec.bindings.exchanges) {

        const exchange = rabbitBlock.spec.consumers
            .find((consumer) => consumer.metadata.name === exchangeBindings.exchange);

        if (!exchange) {
            throw new Error(`Could not find exchange ${exchangeBindings.exchange}`);
        }

        const exchangeName = exchange.metadata.name;

        exchanges.push(asExchange(exchange));

        if (!exchangeBindings.bindings?.length) {
            console.warn(`Not binding exchange ${exchangeName} to queues because there are no bindings`);
            continue;
        }

        for (const bindings of exchangeBindings.bindings) {
            if (bindings.type !== 'queue' ||
                bindings.name !== queue.metadata.name) {
                // Not for this queue
                continue;
            }

            if (typeof bindings.routing === 'string') {
                console.log(`Binding exchange ${exchangeName} to queue ${queueOptions.queue ?? '<exclusive>'} with routing key ${bindings.routing}`);
                queueBindings.push({
                    exchange: exchangeName,
                    queue: queueOptions.queue,
                    routingKey: bindings.routing,
                })
            } else {
                const headers = bindings.routing?.headers ?? {};
                if (bindings.routing?.matchAll) {
                    headers['x-match'] = 'all';
                } else {
                    headers['x-match'] = 'any';
                }
                queueBindings.push({
                    exchange: exchangeName,
                    queue: queueOptions.queue,
                    routingKey: '',
                    arguments: headers,
                })
                console.log(`Binding exchange ${exchangeName} to queue ${queueOptions.queue ?? '<exclusive>'} with headers`, headers);
            }
        }
    }

    return connection.createConsumer({
        noAck: false, // We want to ack/nack manually
        requeue: opts?.requeue ?? true,
        concurrency: opts?.concurrency,
        qos: opts?.qos,
        consumerTag: `${config.getInstanceId()}_${resourceName}`,
        queue: queueOptions.queue,
        queueOptions,
        queueBindings,
        exchanges
    }, async (msg) => {
        try {
            if (msg.contentEncoding && Buffer.isBuffer(msg.body)) {
                const buf:Buffer = msg.body;
                // For some reason the rabbitmq lib specifically doesn't parse the body if the contentEncoding is set
                // so we do it here
                if (msg.contentType === 'application/json') {
                    msg.body = JSON.parse(buf.toString(msg.contentEncoding as BufferEncoding));
                }

                if (msg.contentType === 'text/plain') {
                    msg.body = buf.toString(msg.contentEncoding as BufferEncoding);
                }
            }

            const result = await callback(msg.body, msg);
            switch (result) {
                case ConsumerStatus.REQUEUE:
                case ConsumerStatus.ACK:
                case ConsumerStatus.DROP:
                    return result;
            }
            return ConsumerStatus.ACK;
        } catch (e: any) {
            if (e === DROP_MESSAGE) {
                return ConsumerStatus.DROP;
            }
            console.error('Failed to handle message', e);
            return ConsumerStatus.REQUEUE;
        }
    })
}