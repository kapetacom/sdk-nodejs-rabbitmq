import { ResourceWithSpec } from '@kapeta/ui-web-types';
import { Entity, EntityList, Metadata } from '@kapeta/schemas';

export interface PayloadType {
    type: string;
    structure: Entity;
}

export interface RabbitMQBaseSpec {
    port: {
        type: 'amqp';
    };
    payloadType: PayloadType;
}

export interface RabbitMQSubscriberSpec extends RabbitMQBaseSpec {}

export interface RabbitMQPublisherSpec extends RabbitMQBaseSpec {}

export interface RabbitMQExchangeSpec extends RabbitMQBaseSpec {
    exchangeType: 'direct' | 'fanout' | 'topic' | 'headers';
    alternateExchange?: string;
    internal?: boolean;
    arguments?: any;
    durable?: boolean;
    autoDelete?: boolean;

}

export interface RabbitMQQueueSpec extends RabbitMQBaseSpec {
    arguments?: any;
    deadLetterExchange?: string;
    deadLetterRoutingKey?: string;
    maxPriority?: number;
    maxLength?: number;
    messageTtl?: number;
    expires?: number;
    durable?: boolean;
    exclusive?: boolean;
    autoDelete?: boolean;
}

export interface HeaderBindings {
    matchAll: boolean;
    headers: { [key: string]: string };
}

export type QueueRouting = string | HeaderBindings;

export interface QueueBindingSchema {
    queue: string;
    routing?: QueueRouting;
}

export interface ExchangeBindingsSchema {
    exchange: string;
    bindings?: QueueBindingSchema[];
}

export interface RabbitMQBindingsSchema {
    exchanges?: ExchangeBindingsSchema[];
}

export interface RabbitMQBlockSpec {
    entities?: EntityList;
    consumers?: RabbitMQExchangeResource[];
    providers?: RabbitMQQueueResource[];
    bindings?: RabbitMQBindingsSchema;
}

export interface RabbitMQBlockDefinition {
    kind: string;
    metadata: Metadata;
    spec: RabbitMQBlockSpec;
}

export interface RabbitMQSubscriberResource extends ResourceWithSpec<RabbitMQSubscriberSpec> {}

export interface RabbitMQPublisherResource extends ResourceWithSpec<RabbitMQPublisherSpec> {}

export interface RabbitMQExchangeResource extends ResourceWithSpec<RabbitMQExchangeSpec> {}

export interface RabbitMQQueueResource extends ResourceWithSpec<RabbitMQQueueSpec> {}

export type OperatorOptions = {
    vhost:string
}