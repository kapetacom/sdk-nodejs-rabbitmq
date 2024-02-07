import {ConfigProvider} from "@kapeta/sdk-config";
import {RabbitMQBlockDefinition, RabbitMQExchangeResource} from "./types";
import {
    asExchange,
    asQueue,
    connectToInstance,
    exchangeBindingEnsure,
    exchangeEnsure,
    queueBindingEnsure,
    queueEnsure
} from "./shared";
import {Connection, Publisher as AMQPPublisher} from "rabbitmq-client";
import {Cmd, Envelope, HeaderFields, MethodParams} from "rabbitmq-client/lib/codec";

type TargetExchange = {
    exchange: string,
    publisher: AMQPPublisher
}

export type PublishOptions = Omit<Envelope, 'headers' | 'contentType' | 'appId' | 'contentEncoding' | 'routingKey'>

export interface PublishData<DataType = any, Headers = {}, RoutingKey = string> {
    data: DataType
    routingKey?: RoutingKey
    headers?: HeaderFields['headers'] & Headers
}

export interface Publisher<DataType = any, Headers = {}, RoutingKey = string> {
    publish(data: PublishData<DataType, Headers, RoutingKey>, options?: PublishOptions): Promise<void>;

    close(): Promise<void>;
}


export interface PublisherOptions {
    /**
     * Whether to use a confirm channel or not. Note that this will be slower
     */
    confirm?: boolean

    /**
     * The maximum number of attempts to publish a message.
     */
    maxAttempts?: number
}


/**
 *
 * @param config the configuration provider
 * @param resourceName the name of the publisher resource
 * @param publisherOptions options for the publisher
 */
export async function createPublisher<DataType = any, Headers = {}, RoutingKey = string>(config: ConfigProvider, resourceName: string, publisherOptions?: PublisherOptions): Promise<Publisher<DataType, Headers, RoutingKey>> {
    const targets: TargetExchange[] = []
    const connections: { [key: string]: Connection } = {}
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

        const consumers = rabbitBlock.spec.consumers;
        const exchangeBindingDefinitions = rabbitBlock.spec.bindings.exchanges;

        if (!connections[instance.instanceId]) {
            connections[instance.instanceId] = await connectToInstance(config, instance.instanceId);
        }

        const connection = connections[instance.instanceId];

        const exchanges: Array<MethodParams[Cmd.ExchangeDeclare]> = [];
        const exchangeBindings: Array<MethodParams[Cmd.ExchangeBind]> = [];

        // Get the defined exchanges that this publisher should publish to
        const exchangeDefinitions = instance.connections.map((connection) => {
            return rabbitBlock.spec.consumers?.find((consumer) => {
                return consumer.metadata.name === connection.consumer.resourceName &&
                    connection.provider.resourceName === resourceName;
            });
        }).filter(Boolean) as RabbitMQExchangeResource[];

        // Create defined exchanges. These are the ones that are defined in the block
        for (const exchange of exchangeDefinitions) {

            const exchangeName = exchange.metadata.name;
            exchanges.push(asExchange(exchange));

            for (const bindings of exchangeBindingDefinitions) {
                if (!bindings.bindings ||
                    bindings.exchange !== exchange.metadata.name) {
                    continue;
                }

                for (const binding of bindings.bindings) {
                    if (binding.type !== 'exchange') {
                        // Not for this exchange
                        continue;
                    }

                    const boundExchange = consumers.find(c => c.metadata.name === binding.name);
                    if (!boundExchange) {
                        throw new Error(`Could not find exchange ${binding.name}`);
                    }
                    const boundExchangeName = boundExchange.metadata.name;
                    exchanges.push(asExchange(boundExchange));

                    if (typeof binding.routing === 'string') {
                        console.log(`Binding exchange ${exchangeName} to exchange ${boundExchangeName} with routing key ${binding.routing}`);
                        exchangeBindings.push({
                            routingKey: binding.routing,
                            arguments: {},
                            source: exchangeName,
                            destination: boundExchangeName,
                        });
                    } else {
                        const headers = binding.routing?.headers ?? {};
                        if (binding.routing?.matchAll) {
                            headers['x-match'] = 'all';
                        } else {
                            headers['x-match'] = 'any';
                        }
                        console.log(`Binding exchange ${exchangeName} to exchange ${boundExchangeName} with headers`, headers);
                        exchangeBindings.push({
                            routingKey: '',
                            arguments: headers,
                            source: exchangeName,
                            destination: boundExchangeName,
                        });
                    }
                }
            }

            targets.push({
                exchange: exchangeName,
                publisher: connection.createPublisher({
                    confirm: publisherOptions?.confirm ?? false,
                    maxAttempts: publisherOptions?.maxAttempts,
                    exchanges,
                    exchangeBindings
                })
            });
        }

    }


    return {

        async publish(data: PublishData<DataType, Headers, RoutingKey>, options?: PublishOptions) {
            const content = Buffer.from(JSON.stringify(data.data));
            const promises: Promise<void>[] = []
            const publishOptions: Envelope = {
                ...options,
                contentType: 'application/json',
                contentEncoding: 'utf8',
                appId: `${config.getInstanceId()}_${resourceName}`,
                headers: data?.headers,
                routingKey: (data?.routingKey ?? '').toString().toLowerCase()
            };

            // Publish to all target exchanges. These might be defined spread across multiple servers
            for (const target of targets) {
                console.log(`Publishing to exchange ${target.exchange} with routing key ${publishOptions.routingKey}`);

                promises.push(target.publisher.send({
                    ...publishOptions,
                    exchange: target.exchange,
                }, data.data));
            }

            await Promise.all(promises);
        },
        async close() {
            for (const target of targets) {
                try {
                    await target.publisher.close();
                } catch (e) {
                    console.error('Failed to close publisher', e);
                }
            }
        }
    }
}

