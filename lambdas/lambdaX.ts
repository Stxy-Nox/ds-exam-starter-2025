import { Handler, SQSEvent } from "aws-lambda";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({ region: process.env.REGION });

export const handler: Handler = async (event: SQSEvent, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    for (const record of event.Records) {
      console.log("Processing record:", record.body);
      
   
      const message = JSON.parse(record.body);
    
      if (message.Message) {
        const s3Event = JSON.parse(message.Message);
        
     
        if (s3Event.Records && s3Event.Records.length > 0) {
          const s3Record = s3Event.Records[0];
          const bucket = s3Record.s3.bucket.name;
          const key = s3Record.s3.object.key;
          
          
          const processedMessage = {
            source: "lambdaX",
            timestamp: new Date().toISOString(),
            data: {
              bucket: bucket,
              key: key,
              size: s3Record.s3.object.size,
              eventTime: s3Record.eventTime
            }
          };
          
          
          const queueBUrl = process.env.QUEUE_B_URL;
          
          if (queueBUrl) {
            
            await sqsClient.send(new SendMessageCommand({
              QueueUrl: queueBUrl,
              MessageBody: JSON.stringify(processedMessage)
            }));
            
            console.log(`Message sent to QueueB: ${JSON.stringify(processedMessage)}`);
          } else {
            console.error("QUEUE_B_URL environment variable is not defined");
          }
        }
      }
    }
    
  } catch (error: any) {
    console.error("Error processing message:", error);
    throw new Error(JSON.stringify(error));
  }
};
