import {Message, Options} from "amqplib";
import {ConfigProvider} from "@kapeta/sdk-config";
import {RabbitMQBlockDefinition, RabbitMQQueueResource} from "./types";
import {connectToInstance} from "./shared";

interface ConsumerOptions extends Omit<Options.Consume,'noAck'|'consumerTag'> {
    preFetch?: number,
}

export type Consumer<DataType = any> = (data: DataType, raw: Message) => void | Promise<void>;


export async function createConsumer<DataType = any>(config: ConfigProvider, resourceName:string, callback: Consumer<DataType>, opts?:ConsumerOptions) {
    const instance = await config.getInstanceForConsumer(resourceName);
    if (!instance) {
        throw new Error(`Could not find instance for consumer ${resourceName}`);
    }
    const rabbitBlock = instance.block as RabbitMQBlockDefinition;

    if (!rabbitBlock.spec.consumers?.length ||
        !rabbitBlock.spec.providers?.length ||
        !rabbitBlock.spec.bindings?.exchanges?.length) {
        throw new Error('Invalid rabbitmq block definition. Missing consumers, providers and/or bindings');
    }

    const prefix = `${instance.instanceId}`;
    const channel = await connectToInstance(config, instance.instanceId)
        .then((connection) => {
            return connection.createChannel();
        });

    // Get the defined exchanges that this publisher should publish to
    const blockResources = instance.connections.map((connection) => {
        return rabbitBlock.spec.providers?.find((consumer) => {
            return consumer.metadata.name === connection.provider.resourceName;
        });
    }).filter(Boolean) as RabbitMQQueueResource[];

    if (blockResources.length === 0) {
        throw new Error('No defined queues found');
    }

    if (blockResources.length > 1) {
        throw new Error('Multiple defined queues found. Only 1 expected');
    }

    const queue = blockResources[0];

    const queueRequestName = queue.spec.exclusive ? '' : `${prefix}_${queue.metadata.name}`;
    console.log(`Asserting queue ${queueRequestName || '<exclusive>'}`);
    const assertedQueue = await channel.assertQueue(queueRequestName, {
        durable: queue.spec.durable,
        autoDelete: queue.spec.autoDelete,
        expires: queue.spec.expires,
        messageTtl: queue.spec.messageTtl,
        exclusive: queue.spec.exclusive,
        maxLength: queue.spec.maxLength,
        maxPriority: queue.spec.maxPriority,
        deadLetterRoutingKey: queue.spec.deadLetterRoutingKey,
        deadLetterExchange: queue.spec.deadLetterExchange,
        arguments: queue.spec.arguments,
    });

    const queueName = assertedQueue.queue;

    // Bind exchanges to queue
    for (const exchangeBindings of rabbitBlock.spec.bindings.exchanges) {
        const exchangeName = `${prefix}_${exchangeBindings.exchange}`;
        if (!exchangeBindings.bindings?.length) {
            console.warn(`Not binding exchange ${exchangeName} to queues because there are no bindings`);
            continue;
        }

        for (const queueBinding of exchangeBindings.bindings) {
            if (queueBinding.queue !== queue.metadata.name) {
                // Not for this queue
                continue;
            }

            if (typeof queueBinding.routing === 'string') {
                console.log(`Binding exchange ${exchangeName} to queue ${queueName} with routing key ${queueBinding.routing}`);
                await channel.bindQueue(queueName, exchangeName, queueBinding.routing);
            } else {
                const headers = queueBinding.routing?.headers ?? {};
                if (queueBinding.routing?.matchAll) {
                    headers['x-match'] = 'all';
                } else {
                    headers['x-match'] = 'any';
                }
                console.log(`Binding exchange ${exchangeName} to queue ${queueName} with headers`, headers);
                await channel.bindQueue(queueName, exchangeName, '', headers);
            }
        }
    }

    if (opts?.preFetch) {
        await channel.prefetch(opts.preFetch);
        delete opts.preFetch;
    }

    try {
        await channel.consume(queueName, async (msg) => {
            if (!msg) {
                return;
            }

            let data:DataType;
            try {
                const content = msg.content.toString('utf8');
                data = JSON.parse(content);
            } catch (e) {
                console.error('Could not parse message', e);
                channel.nack(msg, false, false);
                return;
            }

            try {
                await callback(data, msg);
                channel.ack(msg);
            } catch (e) {
                console.error('Failed to handle message', e);
                channel.nack(msg, false, true);
            }
        }, {
            noAck: true,
            consumerTag: `${config.getInstanceId()}_${resourceName}`,
            ...opts,
        });
    } catch (e) {
        channel.close().catch(() => {});
        throw e;
    }

    return async () => {
        await channel.close();
    };

}