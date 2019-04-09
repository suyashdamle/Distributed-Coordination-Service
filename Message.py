class Message(object):
	"""
	Objects of Message Class are sent across the threads and into.
	All variables are Protected type - accessible using object

	"""

	def __init__(self, m_type, msg_id=None, source_host = None, source_port = None,
				recv_host = None, recv_port = None, data_dict = None):
		"""
		Params:
			m_type = (int type) message type, as per agreed-upon enum
			recv_host = (string type) receiver's host; pass None if it is to be returned to default receiver
			recv_port = (string type) receiver's port#; pass None if it is to be returned to default receiver
			source_host = (string type) source's host
			source_port = (string type) source's port#;
			data_dict = (dict type) associated data to be sent
		"""
		self._recv_host = recv_host
		self._recv_port = recv_port
		self._source_host = source_host
		self._source_port = source_port
		self._m_type = m_type
		self._msg_id = msg_id
		self._data_dict = data_dict

	def get_data(self,field):
		"""
		params: field = for a particular data field in data_dict; None for whole dict

		"""
		if field is None:
			return self._data_dict
		else :
			if field not in self._data_dict:
				return None
			else:
				return self._data_dict[field]
