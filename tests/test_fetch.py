from unittest import TestCase
from fetch import fetch_usb, fetch_city, fetch_envirowatch


class TestFetch(TestCase):
    def test_fetch_usb(self):
        fetch_usb()

    def test_fetch_city(self):
        fetch_city()

    def test_fetch_envirowatch(self):
        fetch_envirowatch()
