class Directory(object):
	def __init__(self, dir_type=0, path=None, version_no=-1, file_list={}, lock=False):
		self._type = dir_type		#type = 0 for Dir, 1 for file
		self._path = path
		self._version_no = version_no
		self._file_list = file_list
		self._lock = lock