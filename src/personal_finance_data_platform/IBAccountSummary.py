from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import json
import threading

HOST = "127.0.0.1"      # localhost
PORT = 4001             # IB Gateway default port
CLIENT_ID = 0           # a random integer


class IBAccountSummary(EWrapper, EClient):
    """
    Extract data from the Interactive Brokers Gateway API.
    Returns data for all available accounts: account ID, account type, currency, net liquidation value, total cash value, accrued cash and gross position value.
    net liquidation value = total cash value + accrued cash + gross position value.
    This works only if IB Gateway is installed and logged in.
    """
    def __init__(self):
        EClient.__init__(self, self)
        # Event to signal that the account summary request is complete
        self.account_summary_event = threading.Event()
        self.req_id = 0
        # Initialize a dictionary to aggregate account data
        self.accounts_data = {}
        # Initialize current_time to prevent the race condition
        self.current_time = 0

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        # Store the next valid ID for our request
        self.req_id = orderId
        self.start()
        # Request the server time once at the beginning
        self.reqCurrentTime()

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        # Aggregate the data as it comes in
        if account not in self.accounts_data:
            self.accounts_data[account] = {"currency": ""}  
        if currency:
            self.accounts_data[account]["currency"] = currency
        self.accounts_data[account][tag] = value

    def currentTime(self, time: int):
        super().currentTime(time)
        # Store the server time when it's received
        self.current_time = time

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        # Now that all data is received, add the timestamp and print
        for account_id in self.accounts_data:
            self.accounts_data[account_id]['time'] = self.current_time

        # Signal that the data is received and we can disconnect
        self.account_summary_event.set()

    def start(self):
        # A comprehensive list of tags for account summary.
        # See API documentation for all available tags: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-ref/#acctsumtags-ref
        tags = "AccountType,NetLiquidation,TotalCashValue,AccruedCash,GrossPositionValue"
        # Use "All" to get a summary for all accounts.
        # You could also specify a comma-separated list of account IDs: "U12345,U67890"
        self.reqAccountSummary(self.req_id, "All", tags)

def main():
    # 1. (Main Thread) Create the IBAccountSummary instance
    app = IBAccountSummary()
    
    # 2. (Main Thread) Send a "connect" request to the server and immediately continue.
    app.connect(HOST, PORT, clientId=CLIENT_ID)

    # 3. (Main Thread) Create a new thread for our "Listener".
    #    We tell it to run the app.run() method, which contains the infinite loop
    #    for processing incoming messages. `daemon=True` means this thread
    #    will shut down automatically when the main script exits.
    api_thread = threading.Thread(target=app.run, daemon=True)
    
    # 4. (Main Thread) Start the "Listener" thread. It now runs in the background.
    api_thread.start()

    # 5. (Main Thread) Now, the main thread pauses and waits. The `wait()` method
    #    blocks execution until another thread calls `app.account_summary_event.set()`.
    print("Requesting account summary...")
    app.account_summary_event.wait(timeout=10) # Wait for up to 10 seconds

    # --- Meanwhile, in the background ---
    # 6. (Listener Thread) After connecting, the listener thread calls `nextValidId`, which sends the account summary request.
    # 7. (Listener Thread) The `app.run()` loop receives data messages, calling `accountSummary` for each,
    #    and finally calls `accountSummaryEnd` when the data stream is complete.
    # 8. (Listener Thread) Inside `accountSummaryEnd`, `self.account_summary_event.set()` is called.
    # ------------------------------------

    # 9. (Main Thread) The event is now "set", so `wait()` unblocks and the main
    #    script continues execution, now confident that all data is received.

    app.disconnect()
    print("Disconnected.\n")
    print("Extracted Data:")
    print(json.dumps(app.accounts_data, indent=2))

if __name__ == "__main__":
    main()
