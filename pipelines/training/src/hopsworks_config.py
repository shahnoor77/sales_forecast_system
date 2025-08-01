import hopsworks



def init_hopsworks(api_key: str, project_name: str):
    project = hopsworks.login(api_key_value=api_key, project=project_name)
    fs = project.get_feature_store()
    return project, fs


def get_sales_data(fs):
    # Get the feature group
    fg = fs.get_feature_group(name="sales_record", version=1)

    # Select all columns
    query = fg.select_all()
    
    # Try to delete existing feature view if it exists
    try:
        feature_view = fs.get_feature_view(name="sales_record_view", version=1)
        feature_view.delete()
    except:
        pass

    # Create the feature view
    feature_view = fs.create_feature_view(
        name="sales_record_view",
        version=1,
        description="Feature view with all sales data",
        labels=["sub_total"],  # This marks sub_total as a label
        query=query,
    )

    # Read and return the dataframe
    # Make sure to include labels in the batch data
    df = feature_view.get_batch_data(
        include_labels=True,  # This should include the label column
        transformation=None  # Add this if you're not using transformations
    )
    
    # Verify the columns are present
    print("Columns in retrieved data:", df.columns.tolist())
    
    # If sub_total is still missing, try this alternative approach:
    if 'sub_total' not in df.columns:
        df = fg.read()  # Fall back to reading directly from feature group
        print("Falling back to feature group read, columns:", df.columns.tolist())
    
    return df