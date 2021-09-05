#!/usr/bin/env python3
# RequestDelayer.py - Enforces a minimum delay before an action can be taken
# again. Useful for web scraping and not going to jail.

from functools import wraps
import threading
import time
from typing import Callable

class RequestDelayer:
    def __init__(self, delay: float):
        self.delay = delay
        self.lastRequest = 0.0
        self.delayLock = threading.RLock()

    def delayRequest(self, function: Callable) -> Callable:
        """Delays the a request by a certain delay amongst all threads"""
        
        @wraps(function)
        def wrapper(*args, **kwargs):
            with self.delayLock:
                current = time.time()
                wait = self.lastRequest + self.delay - current
                if wait > 0:
                    time.sleep(wait)
                    self.lastRequest = current + wait
                else:
                    self.lastRequest = current
            return function(*args, **kwargs)
        return wrapper