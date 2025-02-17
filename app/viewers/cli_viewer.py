import sys
from controllers.api_controller import APIController

class CLIViewer:
    def __init__(self, controller):
        self.controller = controller

    def display_menu(self):
        while True:

            tables = self.controller.list_tables()
            print("📌 Available Tables:")
            for i, table in enumerate(tables, start=1):
                print(f" {i}. {table}")

            print(f" {len(tables) + 1}. Exit")

            choice = input("Enter the number of the table to view (or exit): ").strip()

            if choice.isdigit():
                choice = int(choice)

                if 1 <= choice <= len(tables):
                    nth_table = tables[choice-1]
                    self.view_table(nth_table)

                elif choice == len(tables) + 1:
                    print("👋 Exiting...")
                    sys.exit(0)

                else:
                    print("⚠️ Invalid choice, please select a valid option.")

            else:
                print("⚠️ Please enter a valid number.")

    def view_table(self, table_name):
        print("__________________________________________________________")
        print(f"\n📊 Viewing Table: {table_name}")
        records = self.controller.read(table_name)
                    
        if records:
            for record in records:
                print(record)
        else:
            print("⚠️ No records found.")
        print("__________________________________________________________")


