import json
import os
import asyncio
from time import sleep

from aiohttp import web, ClientSession
import emails
from emails.template import JinjaTemplate as T
from pika.exceptions import ChannelClosed
import aioredis

from serializers import serialize_body
from models import Transaction, Message, DataBase
from email_sender import Email, check_message_status, check_transaction_status
from rabbit import RabbitConnector

routes = web.RouteTableDef()
base_dir = os.path.abspath(os.path.curdir)


@routes.post('/api/v1/emails')
@serialize_body('post_email')
async def send_email(request: web.Request, body):
    subject = None
    transaction = Transaction(request.app.db)
    if 'subject' in body:
        subject = body['subject']

    template = await read_template(os.path.join(base_dir, '{}.html'.format(body['email_type'])))

    response = await email_sender(body, template, request.app['config'], subject)
    await transaction.save(response['transaction_id'])

    return web.Response(status=200, content_type='application/json', body=json.dumps(response))


async def listen_to_rabbit(app):
    try:
        # rmq = RabbitConnector(app['config'], app.loop)
        # await app.rmq.connect()
        consume_queue = await app.rmq.declare_queue(app['config']['RMQ_CONSUME_QUEUE'])
        email = Email(app['config'])
        print('[*] Waiting for messages. To exit press CTRL+C')
        async for message in consume_queue:
            print('New message')
            message_data = json.loads(message.body.decode())
            try:
                # import ipdb;
                # ipdb.set_trace()
                response = await email.send(message_data)
                # response = {'success': True, 'error': 'Some error'}

                if not response['success']:
                    message_data.update({'error': response['error']})
                    await app.rmq.produce(message_data, app['config']['RMQ_PRODUCER_ERROR'])
                    message.reject()
                    continue

                # response = {'success': True, 'transaction_id': '950daa2c-41de-80dc-c02c-098632a6589e'}

                # await app.db.transaction.save(response['transaction_id'])
                asyncio.ensure_future(
                    check_status(app, response['transaction_id']))
                print('affter')
                message.ack()
                print('accept message')
            except ChannelClosed:
                pass
            except Exception as ex:
                message_data.update({'error': repr(ex)})
                await app.rmq.produce(message_data, app['config']['RMQ_PRODUCER_ERROR'])
                message.reject()
    except (asyncio.CancelledError, KeyboardInterrupt):
        await app.rmq.close()
    except Exception as ex:
        await listen_to_rabbit(app)


async def listen_to_redis(app):
    try:
        redis = app['redis']
        email = Email(app['config'])
        print('Redis wait for message')
        while True:
            if not bool(await redis.llen('email')):
                continue

            message_data = await redis.lpop('email')
            message_data = json.loads(message_data)
            response = await email.send(message_data)
            # response = {'success': True, 'error': 'Some error'}

            if not response['success']:
                message_data.update({'error': response['error']})
                await redis.rpush('email_error', json.dumps(message_data))
                print('Error')
                continue

            # response = {'success': True, 'transaction_id': '950daa2c-41de-80dc-c02c-098632a6589e'}
            asyncio.ensure_future(
                check_status_redis(app, response['transaction_id']))

    except asyncio.CancelledError:
        pass
    except Exception as ex:
        print(ex)
    finally:
        print('Close redis')
        await app['redis'].quit()


async def check_status(app, transaction_id):
    while True:
        print('Sleep ')
        await asyncio.sleep(10)
        status = await check_transaction_status(transaction_id, app['config']['API_KEY'])
        print(status)

        if status['success'] and status['data'].get('messageids'):

            if not (status['data'].get('delivered') or status['data'].get('failed')):
                continue

            await app.rmq.produce({'success': True,
                                   'transaction_id': transaction_id,
                                   'sent': status['data'].get('sent'),
                                   'delivered': status['data'].get('delivered'),
                                   'failed': status['data'].get('failed'),
                                   'messageids': status['data'].get('messageids')},
                                  'email_response')
            break


async def check_status_redis(app, transaction_id):
    redis = app['redis']
    while True:
        print('Sleep ')
        await asyncio.sleep(10)
        status = await check_transaction_status(transaction_id, app['config']['API_KEY'])
        print(status)

        if status['success'] and status['data'].get('messageids'):

            if not (status['data'].get('delivered') or status['data'].get('failed')):
                continue
            print('Try to send message')
            await redis.rpush('email_response', json.dumps({'success': True,
                                                            'transaction_id': transaction_id,
                                                            'sent': status['data'].get('sent'),
                                                            'delivered': status['data'].get('delivered'),
                                                            'failed': status['data'].get('failed'),
                                                            'messageids': status['data'].get('messageids')}))
            print('all done')
            break


@routes.get('/api/v1/transactions')
async def get_all_transactions(request: web.Request):
    transactions = Transaction(request.app.db)
    get_all = await transactions.get_transactions()
    for trans in get_all:
        del trans['_id']
        if 'created' in trans:
            trans['created'] = str(trans['created'])
    return web.Response(status=200, content_type='application/json', body=json.dumps(get_all))


@routes.get('/api/v1/emails/transactions/{transaction_id}')
async def check_email(request: web.Request):
    transaction_id = request.match_info['transaction_id']
    transactions = Transaction(request.app.db)
    trans = await transactions.get_by_id(transaction_id)
    
    if trans['messages']:
        del trans['_id']
        trans['created'] = str(trans['created'])
        return web.Response(status=200, content_type='application/json', body=json.dumps(trans))

    transaction_data = await check_transaction_status(transaction_id, request.app['config']['API_KEY'])

    if not transaction_data['success']:
        raise web.HTTPNotFound(content_type='application/json',
                               body=json.dumps({'error': transaction_data['error']}))

    print(transaction_data)
    trans = await transactions.update(transaction_id, {'messages': transaction_data['data']['messageids'],
                                                       'send_to': transaction_data['data']['sent']})
    
    messages = Message(request.app.db)
    for i, val in enumerate(trans['messages']):
        await messages.save(message_id=val, transaction_id=trans['transaction_id'], send_to=trans['send_to'][i], created=trans['created'], )

    del trans['_id']
    trans['created'] = str(trans['created'])
    return web.Response(status=200, content_type='application/json', body=json.dumps(trans))


@routes.get('/api/v1/emails/messages/{message_id}')
async def get_message_data(request: web.Request):
    message_id = request.match_info['message_id']
    message_data = await check_message_status(message_id, request.app['config']['API_KEY'])

    if not message_data['success']:
        raise web.HTTPNotFound(content_type='application/json',
                               body=json.dumps({'error': message_data['error']}))

    return web.Response(status=200, content_type='application/json', body=json.dumps(message_data))


@routes.get('/api/v1/emails/messages')
async def get_all_messages(request: web.Request):
    messages = Message(request.app.db)
    result = await messages.get_messages()
    for message in result:
        del message['_id']
        message['created'] = str(message['created'])
    return web.Response(status=200, content_type='application/json', body=json.dumps(result))


async def email_sender(body: dict, template: str, config: dict, subject='None'):

    message = emails.html(
        subject=subject,
        html=T(template),
        mail_from=('Ardas Inc', config['EMAIL_ADDRESS'])
    )
    response = message.send(
        to=body['to_addr'],
        render={'to_name': body['to_name'], 'linc': body['linc']},
        smtp={'host': config['SMTP_SERVER'], 'port': config['SMTP_PORT'],
              'tls': True, 'user': config['LOGIN'], 'password': config['PASSWORD']},
    )
    status = response.status_code
    text = response.status_text.decode('utf-8')

    if status != 250:
        raise web.HTTPUnprocessableEntity(content_type='application/json', body=json.dumps({'success': False, 'error': text}))

    return {'success': True, 'transaction_id': text.split()[1]}


async def read_template(filename):
    with open(filename, 'r', encoding='utf-8') as template_file:
        template = template_file.read()
    return template
