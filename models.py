from datetime import datetime

from settings import MESSAGE_COLLECTION, TRANSACTION_COLLECTION


class Transaction:
    
    def __init__(self, collection, **kwargs):
        self.collection = collection
    
    async def save(self, transaction_id, **kwargs):
        result = await self.collection.insert(
            {'transaction_id': transaction_id, 'messages': None, 'send_to': None, 'created': datetime.now()})
        return result
    
    async def get_transactions(self):
        transactions = self.collection.find().sort([('time', 1)])
        return await transactions.to_list(length=None)

    async def get_by_id(self, transaction_id):
        transaction = await self.collection.find_one({'transaction_id': transaction_id})
        return transaction

    async def update(self, transaction_id, data):
        await self.collection.update_one({'transaction_id': transaction_id}, {'$set': data})
        result = await self.collection.find_one({'transaction_id': transaction_id})
        return result


class Message:

    def __init__(self, collection, **kwargs):
        self.collection = collection
    
    async def save(self, message_id, transaction_id, send_to, created, **kwargs):
        result = await self.collection.insert(
            {'message_id': message_id, 'transaction_id': transaction_id, 'send_to': send_to, 'created': created})
        return result
    
    async def get_messages(self):
        result = self.collection.find().sort([('time', 1)])
        return await result.to_list(length=None)

    async def get_by_id(self, message_id):
        result = await self.collection.find_one({'message_id': message_id})
        return result

    async def update(self, message_id, data):
        await self.collection.update_one({'message_id': message_id}, {'$set': data})
        result = await self.collection.find_one({'message_id': message_id})
        return result


class DataBase:

    def __init__(self, db: dict):
        self.transaction = Transaction(db[TRANSACTION_COLLECTION])
        self.message = Message(db[MESSAGE_COLLECTION])
