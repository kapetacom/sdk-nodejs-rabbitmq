import {ConfigProvider} from "@kapeta/sdk-config";
import {connect, Options} from "amqplib";
import {OperatorOptions} from "./types";

export async function connectToInstance(config:ConfigProvider, instanceId:string) {
    const operator = await config.getInstanceOperator<OperatorOptions>(instanceId);
    if (!operator) {
        throw new Error(`No operator found for instance ${instanceId}`);
    }

    const connectOptions:Options.Connect = {
        protocol: operator.protocol,
        hostname: operator.hostname,
        port: operator.port,
        username: operator.credentials?.username,
        password: operator.credentials?.password,
        vhost: operator.options?.vhost ?? '/',
    }

    return connect(connectOptions);
}