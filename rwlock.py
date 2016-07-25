import multiprocessing

class ProcessRWLock:
	def __init__(self):
		self.__monitor	= multiprocessing.Lock()
		self.__exclude	= multiprocessing.Lock()
		self.readers	= 0

	def acquire_read(self):
		with self.__monitor:
			self.readers += 1
			if self.readers == 1:
				self.__exclude.acquire()

	def release_read(self):
		with self.__monitor:
			self.readers -= 1
			if self.readers == 0:
				self.__exclude.release()

	def acquire_write(self):
		self.__exclude.acquire()

	def release_write(self):
		self.__exclude.release()

class ThreadRWLock:
	def __init__(self):
		self.__monitor	= multiprocessing.dummy.Lock()
		self.__exclude	= multiprocessing.dummy.Lock()
		self.readers	= 0

	def acquire_read(self):
		with self.__monitor:
			self.readers += 1
			if self.readers == 1:
				self.__exclude.acquire()

	def release_read(self):
		with self.__monitor:
			self.readers -= 1
			if self.readers == 0:
				self.__exclude.release()

	def acquire_write(self):
		self.__exclude.acquire()

	def release_write(self):
		self.__exclude.release()

