const winston = require('winston');

// Custom Transport for MQTT
class MqttTransport extends winston.Transport {
    constructor(opts) {
        super(opts);
        this.mqttClient = opts.mqttClient;
        this.topic = opts.topic || 'system/logs/dispatch';
    }

    log(info, callback) {
        setImmediate(() => {
            this.emit('logged', info);
        });

        // Only send to MQTT if category is 'dispatch'
        if (this.mqttClient && this.mqttClient.connected && info.category === 'dispatch') {
            const payload = JSON.stringify({
                level: info.level,
                message: info.message,
                timestamp: info.timestamp,
                metadata: info.metadata || {}
            });
            this.mqttClient.publish(this.topic, payload, { qos: 0 });
        }

        callback();
    }
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ timestamp, level, message, category }) => {
            const cat = category ? `[${category.toUpperCase()}] ` : '';
            return `${timestamp} ${level.toUpperCase()}: ${cat}${message}`;
        })
    ),
    transports: [
        new winston.transports.Console()
    ]
});

// Function to attach MQTT client to logger
logger.setMqttClient = (client) => {
    logger.add(new MqttTransport({ mqttClient: client }));
};

module.exports = logger;
