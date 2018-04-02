import asyncio
import json

import aioredis


async def main():
    pub = await aioredis.create_redis(
        'redis://localhost')
    data = {
        "to_addr": ["danil.dubovyk@ardas.biz"],
        "to_name": "name",
        "msg": "ESHKERE KurlikKurlik javaSKRSKR ",
        "subject": "many emails",
        "email_type": "restore_password",
        "linc": "www.some.linc"
    }

    await pub.rpush('email', json.dumps(data))
    print('all ok')
    pub.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())