from redis import Redis
from rq import Queue

job_queue = Queue(connection=Redis())
