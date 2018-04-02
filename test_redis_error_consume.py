import asyncio

import aioredis


async def main():
    try:
        redis = await aioredis.create_redis(('localhost', 6379))
        print('Redis wait for message')
        print(await redis.llen('email_response'))
        while True:
            # if bool(await redis.llen('email_response')):
            #     message_data = await redis.lpop('email_response')
            #     print(message_data)
            message_data = await redis.blpop('email_response')
            print(message_data)
    except Exception as ex:
        print(ex)
    finally:
        print('Close redis')
        await redis.quit()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.close()
