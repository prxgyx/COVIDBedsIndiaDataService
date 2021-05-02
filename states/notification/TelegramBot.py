import requests


class TelegramBot():
	def __init__(self):
		self.api_url = "https://api.telegram.org/bot"
		self.ACCESS_TOKEN = "1760399636:AAHIv2PM9mcBFzDdCxY8xM7vAJppkhC90Sg"
		self.chat_id = "-431997622"


	def send_message(self, msg):
		send_message_endpoint = "sendMessage"
		url = self.api_url+self.ACCESS_TOKEN+"/"+send_message_endpoint

		payload = {
			"chat_id": self.chat_id, 
			"text":msg
		}
		requests.post(url, params=payload)


	def send_local_file(self, file_path):
		f = open(file_path, 'rb')

		file_bytes = f.read()
		f.close()
		response = {
			'document': (f.name, file_bytes)
		}
		params={
			'chat_id':self.chat_id
		}
		send_file_endpoint = 'sendDocument'
		url = self.api_url+self.ACCESS_TOKEN+"/"+send_file_endpoint
		requests.post(url=url, params=params, files=response)

