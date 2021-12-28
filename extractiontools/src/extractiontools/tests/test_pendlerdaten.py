import unittest
import orca
from ..steps.extract_data import import_pendlerdaten
from ..injectables.database import database, subfolder_pendlerdaten


class TestImportPendler(unittest.TestCase):
    """Test the import of pendler data"""

    @classmethod
    def patch_injectable(cls, inj: str, value: object):
        wrapper = orca.orca._INJECTABLES[inj]
        cls.backup_injectables[inj] = wrapper._func
        wrapper._func = lambda: value

    @classmethod
    def setUpClass(cls):
        cls.backup_injectables = {}
        patches = {'database': 'test_db', }
        for inj, value in patches.items():
            cls.patch_injectable(inj, value)

    @classmethod
    def tearDownClass(cls):
        for inj, func in cls.backup_injectables.items():
            orca.orca._INJECTABLES[inj]._func = func

    def test_import(self):
        """Test the import"""
        step = orca.get_step('import_pendlerdaten')
        step()



