import {ConfigProvider} from "@kapeta/sdk-config";
import {ConfirmChannel, MessagePropertyHeaders, Options} from "amqplib";
import {RabbitMQBlockDefinition, RabbitMQExchangeResource} from "./types";
import {connectToInstance} from "./shared";

type TargetExchange = {
    exchange: string,
    channel: ConfirmChannel
}

export type PublisherOptions = Omit<Options.Publish, 'headers' | 'contentType'| 'appId'| 'contentEncoding'>

export interface PublishData<DataType = any, Headers = MessagePropertyHeaders> {
    data: DataType
    routingKey?: string
    headers?: Headers
}

export interface Publisher<DataType = any, Headers = MessagePropertyHeaders> {
    publish(data: PublishData<DataType,Headers>, options?: PublisherOptions): Promise<void>;
    close(): Promise<void>;
}

export async function createPublisher<DataType = any, Headers = MessagePropertyHeaders>(config: ConfigProvider, resourceName:string):Promise<Publisher<DataType, Headers>> {
    const targetExchanges: TargetExchange[] = []
    const channels: { [key: string]: ConfirmChannel } = {}
    const instances = await config.getInstancesForProvider(resourceName);
    if (instances.length === 0) {
        throw new Error(`No instances found for provider ${resourceName}`);
    }

    for (const instance of instances) {
        const rabbitBlock = instance.block as RabbitMQBlockDefinition;

        if (!rabbitBlock.spec.consumers?.length ||
            !rabbitBlock.spec.providers?.length ||
            !rabbitBlock.spec.bindings?.exchanges?.length) {
            throw new Error('Invalid rabbitmq block definition. Missing consumers, providers and/or bindings');
        }

        if (!channels[instance.instanceId]) {
            channels[instance.instanceId] = await connectToInstance(config, instance.instanceId)
                .then((connection) => {
                    return connection.createConfirmChannel();
                });
        }

        const channel = channels[instance.instanceId];
        const prefix = `${instance.instanceId}`;

        // Get the defined exchanges that this publisher should publish to
        const blockResources = instance.connections.map((connection) => {
            return rabbitBlock.spec.consumers?.find((consumer) => {
                return consumer.metadata.name === connection.consumer.resourceName
            });
        }).filter(Boolean) as RabbitMQExchangeResource[];

        // Create defined exchanges. These are the ones that are defined in the block
        for (const exchange of blockResources) {
            const exchangeName = `${prefix}_${exchange.metadata.name}`;
            console.log(`Asserting defined exchange ${exchangeName}`);
            await channel.assertExchange(exchangeName, exchange.spec.exchangeType, {
                durable: exchange.spec.durable,
                autoDelete: exchange.spec.autoDelete,
                alternateExchange: exchange.spec.alternateExchange,
                internal: exchange.spec.internal,
                arguments: exchange.spec.arguments,
            });

            targetExchanges.push({
                channel,
                exchange: exchangeName
            });
        }
    }

    return {

        async publish(data: PublishData<DataType,Headers>, options?: PublisherOptions) {
            const content = Buffer.from(JSON.stringify(data.data));
            const promises: Promise<void>[] = []
            // Publish to all target exchanges. These might be defined spread across multiple servers
            for (const target of targetExchanges) {
                promises.push(new Promise<void>((resolve, reject) => {
                    target.channel.publish(target.exchange, data?.routingKey ?? '', Buffer.from(content), {
                        ...options,
                        headers: data?.headers,
                        contentType: 'application/json',
                        contentEncoding: 'utf8',
                        appId: `${config.getInstanceId()}_${resourceName}`,
                    }, (err) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve();
                    });
                }));
            }

            await Promise.all(promises);
        },
        async close() {
            for (const target of targetExchanges) {
                try {
                    await target.channel.close();
                } catch (e) {
                    console.error('Failed to close channel', e);
                }
            }
        }
    }
}

