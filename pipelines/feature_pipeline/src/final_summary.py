# print details from final_Standardized_.csv
import pandas as pd
def print_final_summary():
    # Load the final standardized data from "./data/transformed/final_Standardized_.csv"
    print("Loading final_Standardized_.csv...")
    final_data = pd.read_csv("./data/transformed/final_Standardized.csv")
    
    # Print the first few rows of the dataframe
    print("First few rows of final_Standardized_.csv:")
    print(final_data.head())
    
    # Print summary statistics
    print("\nSummary statistics:")
    print(final_data.describe())
    
    # Print the shape of the dataframe
    print("\nShape of the dataframe:", final_data.shape)

# Call the function to print the summary
if __name__ == "__main__":  
    print_final_summary()
