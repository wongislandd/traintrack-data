from gtfs_utils import create_supabase_client

def main():
    supabase = create_supabase_client()

    print("Populating routes_stops table...")

    # Call the function instead of passing raw SQL
    result = supabase.rpc("populate_routes_stops").execute()

    if result.data is None:
        print("RPC returned no data")
    else:
        print("RPC result:", result.data)

if __name__ == "__main__":
    main()