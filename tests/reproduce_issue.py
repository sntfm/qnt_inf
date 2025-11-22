import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add jobs directory to path so we can import the script
sys.path.append(os.path.join(os.path.dirname(__file__), '../jobs'))

# Import the module to test
# We need to mock psycopg2 before importing if it's imported at top level, 
# but here it's imported inside functions or at top level. 
# The script imports psycopg2 at top level.
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

import importlib
# We need to import the script as a module. 
# Since it has a hyphen, we use import_module
srv_mart_decay_slices_update = importlib.import_module('srv-mart_decay_slices-update')

class TestDecaySlicesUpdate(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cur = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cur
        
        # Setup common test data
        self.deal = {
            'time': '2025-10-20T10:00:00.123456Z',
            'instrument': 'BTC/USD',
            'side': 'BUY',
            'amt': 1.5,
            'px': 50000.0
        }
        
        self.convmap = {
            'BTC/USD': ('USD', False), # Direct
            'ETH/BTC': ('BTC/USD', False), # Cross
            'EUR/USD': ('USD', True) # Inverted example if needed
        }

    def test_process_deal_direct_usd(self):
        # Test case where usd_info is None (already USD or no conversion)
        # In the script, if usd_info is None, it does the simple insert
        # Let's test the case where it IS in convmap but maybe direct?
        # Actually the script checks `if usd_info is None`.
        # If 'BTC/USD' is in convmap, it goes to else block.
        
        # Let's test the problematic path: usd_info is NOT None
        # Case 1: Normal conversion (not inverted)
        # convmap has 'BTC/EUR' -> ('EUR/USD', False)
        
        deal = {
            'time': '2025-10-20T10:00:00.123456Z',
            'instrument': 'BTC/EUR',
            'side': 'BUY',
            'amt': 1.0,
            'px': 40000.0
        }
        convmap = {'BTC/EUR': ('EUR/USD', False)}
        
        # We need to mock _fetch_price_at if we implement it, 
        # but right now we are testing the EXISTING broken logic or the NEW logic?
        # This test is to verify the FIX. So I will write the test expecting the FIX.
        
        # Mock the cursor for _fetch_price_at and the main query
        # First call is _fetch_price_at, second is the INSERT
        
        # We need to configure the mock to return different values for different calls
        # _fetch_price_at calls fetchone()
        # The INSERT doesn't call fetchone() (it just executes)
        
        # Mock return value for _fetch_price_at
        # Returns (ask, bid)
        self.mock_cur.fetchone.side_effect = [(1.1, 1.0)] 
        
        srv_mart_decay_slices_update._process_deal(self.mock_cur, deal, convmap)
        
        # Verify _fetch_price_at was called
        # It executes a SELECT ... LIMIT 1
        self.assertTrue(self.mock_cur.execute.called)
        
        # Check the SQL calls
        # We expect 2 execute calls:
        # 1. SELECT ask_px_0, bid_px_0 ...
        # 2. INSERT INTO ...
        
        calls = self.mock_cur.execute.call_args_list
        self.assertEqual(len(calls), 2)
        
        fetch_sql = calls[0][0][0]
        insert_sql = calls[1][0][0]
        
        # Verify fetch SQL
        self.assertIn("SELECT ask_px_0, bid_px_0", fetch_sql)
        self.assertIn("LIMIT 1", fetch_sql)
        
        # Verify insert SQL
        # Should NOT contain CROSS JOIN ... e
        self.assertNotIn("CROSS JOIN", insert_sql)
        # Should contain calculated pnl_usd
        # entry_usd = 40000 * 1.0 * 1.0 (since rate is 1.0/1.0 from mock) = 40000
        # pnl_usd formula in SQL: ... - 40000.0 pnl_usd
        self.assertIn("40000.0", insert_sql)
        
        print("Verified SQL generation is correct and does not use exact timestamp join.")

    def test_fetch_price_at(self):
        # This will test the new helper function
        pass

if __name__ == '__main__':
    unittest.main()
