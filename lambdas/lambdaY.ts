import { Handler, SQSEvent } from "aws-lambda";
import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";

const dynamoClient = new DynamoDBClient({ region: process.env.REGION });
const TABLE_NAME = process.env.TABLE_NAME;

export const handler: Handler = async (event: SQSEvent, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    for (const record of event.Records) {
      console.log("处理来自 QueueB 的记录:", record.body);
      
      const message = JSON.parse(record.body);
      
      // 验证消息是否符合要求：没有 email 属性，但有 country 属性
      if ('email' in message) {
        console.warn("警告: 收到包含 email 属性的消息，这不符合要求");
        continue; // 跳过处理这条消息
      }
      
      if (!('country' in message)) {
        console.warn("警告: 收到缺少 country 属性的消息，这不符合要求");
        continue; // 跳过处理这条消息
      }
      
      console.log(`处理来自 S3 的文件: ${message.data.bucket}/${message.data.key}`);
      console.log(`文件大小: ${message.data.size} 字节`);
      console.log(`处理时间: ${message.timestamp}`);
      console.log(`国家: ${message.country}`);
      
      if (TABLE_NAME) {
        const logId = `${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        
        try {
          await dynamoClient.send(new UpdateItemCommand({
            TableName: TABLE_NAME,
            Key: {
              movieId: { N: "9999" }, 
              role: { S: `file-process-log-${logId}` }
            },
            UpdateExpression: "SET #src = :src, #ts = :ts, #data = :data, #country = :country",
            ExpressionAttributeNames: {
              "#src": "source",
              "#ts": "timestamp",
              "#data": "data",
              "#country": "country"
            },
            ExpressionAttributeValues: {
              ":src": { S: message.source },
              ":ts": { S: message.timestamp },
              ":data": { S: JSON.stringify(message.data) },
              ":country": { S: message.country }
            }
          }));
          
          console.log("文件处理记录已保存到 DynamoDB");
        } catch (dbError) {
          console.error("保存到 DynamoDB 时出错:", dbError);
        }
      }
    }
  } catch (error: any) {
    console.error("处理消息时出错:", error);
    throw new Error(JSON.stringify(error));
  }
};
