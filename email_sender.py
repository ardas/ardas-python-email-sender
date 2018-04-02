import os
import asyncio
from time import sleep

import emails
from emails.template import JinjaTemplate as T
from aiohttp import ClientSession
import aiohttp

from models import DataBase


class Email:
    def __init__(self, config: dict):
        self.template_dir = config['TEMPLATE_DIR']
        self.email_address = config['EMAIL_ADDRESS']
        self.smtp_server = config['SMTP_SERVER']
        self.smtp_port = config['SMTP_PORT']
        self.login = config['LOGIN']
        self.password = config['PASSWORD']

    async def send(self, body):
        template = await self._read_template(body['email_type'])
        response = await self._send(body, template)

        return response

    async def _read_template(self, filename: str):
        template_path = os.path.join(self.template_dir, '{}.html'.format(filename))
        with open(template_path, 'r', encoding='utf-8') as template_file:
            template = template_file.read()
        return template

    async def _send(self, body: dict, template: str) -> dict:
        subject = body.get('subject', '')
        message = emails.html(
            subject=subject,
            html=T(template),
            mail_from=('Ardas Inc', self.email_address)
        )
        response = message.send(
            to=body['to_addr'],
            render={'to_name': body['to_name'], 'linc': body['linc']},
            smtp={'host': self.smtp_server, 'port': self.smtp_port,
                  'tls': True, 'user': self.login, 'password': self.password},
        )
        status = response.status_code
        text = response.status_text.decode('utf-8')

        if status != 250:
            return {'success': False, 'error': text}

        return {'success': True, 'transaction_id': text.split()[1]}


async def check_transaction_status(transaction_id: str, api_key: str):
    params = {
        'transactionID': transaction_id,
        'apikey': api_key,
        'showFailed': 'true',
        'showErrors': 'true',
        'showMessageIDs': 'true',
        'showAbuse': 'true',
        'showClicked': 'true',
        'showDelivered': 'true',
        'showOpened': 'true',
        'showPending': 'true',
        'showSent': 'true',
        'showUnsubscribed': 'true',
    }

    async with ClientSession() as session:
        async with session.get('https://api.elasticemail.com/v2/email/getstatus', params=params) as response:
            return await response.json()


async def check_message_status(message_id: str, api_key: str):
    params = {
        'apikey': api_key,
        'messageID': message_id,
    }
    async with ClientSession() as session:
        async with session.get('https://api.elasticemail.com/v2/email/status', params=params) as response:
            return await response.json()
