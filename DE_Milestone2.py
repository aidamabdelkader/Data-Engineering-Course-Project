### Data Engineering Milestone 2 
from ast import operator
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator ## we have 
## multiple python operators to be implemented
from datetime import datetime 
import pandas as pd
import numpy as np 

########################################################################
## First, will start defining all the functions that will be used across the DAG 

## defining the function that will read the data 

def loading_Data(path): 
    df = pd.read_csv(path)
    return df

## renaming the data columns 

def Rename_columns(df):
    df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    return df

## define the function for removing duplicates 


def Removing_duplicates(df):
    df = df.drop_duplicates()
    return df

## define the function for tidying the columns 

def Tidying_columns(df):
    for column in df.columns:
        if df[column].min() == -1:
            df[column] = df[column].replace(-1, np.NaN)
        elif df[column].min() == "-1" or df[column].astype(str).str.lower().eq('unknown').any():
            df[column] = df[column].replace("-1", np.NaN)
            df[column] = df[column].replace("unknown", np.NaN)
        elif df[column].min() == "-1" or df[column].astype(str).eq("Unknown / Non-Applicable").any():
            df[column] = df[column].replace("-1", np.NaN)
            df[column] = df[column].replace("Unknown / Non-Applicable", np.NaN)
        else:
            pass

    return df

## define the function for dropping non needed columns 

def dropping_non_needed_columns(df, columns_list):
    new_df = df.drop(columns=columns_list, axis=1, inplace=True)
    return new_df

## define the function for removing number from the text 
def Removing_numbers_f_text(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    df[column_name] = df[column_name].replace(['\d', '\n.'], '', regex=True)
    return df


## define the function for removing special character from the columns 

def Special_characters_removed(df,column_name):
    
    '''takes column name and activates
    then split over the column 
    creating new minmum and maximum per column 
    '''
    
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    # Access the specified column
    selected_column = df[column_name]

    # Split the column based on '('
    df[["first_part", "second_part"]] = selected_column.str.split('(', n=1, expand=True)

    # Remove 'k' and '$" 
    df[column_name + "_" + "Adjusted"] = df["first_part"].replace(['K', '\$'], "", regex=True)
    
    ## Splitting the column to have minmum value and maximum value 
    df[[column_name + "_" + "Min", column_name + "_" + "Max"]] = df[column_name + "_" + "Adjusted"].str.split('-', n=1, expand=True)
    
    ### adjusting the type of column minmum and columna maximum into numeric 
    
    df[column_name + "_" + "Min"] = pd.to_numeric(df[column_name + "_" + "Min"], errors='coerce')
    
    df[column_name + "_" + "Max"] = pd.to_numeric(df[column_name + "_" + "Max"], errors='coerce')
    
        
    df[column_name + "_" + "Average"] =  pd.to_numeric(( df[column_name + "_" + "Min"] +  df[column_name + "_" + "Max"]) / 2)

    return df


## define the function for size character removed 

def Size_characters_removed(df, column_name): 
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    # Replace 'to' with '-'
    df[column_name + "_Adjusted"] = df[column_name].str.replace('to', '-')
    
    # Replace 'employees' and '+' with an empty string
    df[column_name + "_Adjusted"] = df[column_name + "_Adjusted"].str.replace('employees', '')
    df[column_name + "_Adjusted"] = df[column_name + "_Adjusted"].str.replace('+', '')

    # Split into minimum and maximum
    df[column_name + "_Min"] = df[column_name + "_Adjusted"].apply(lambda x: x.split('-')[0] if '-' in x else x)
    df[column_name + "_Max"] = df[column_name + "_Adjusted"].apply(lambda x: x.split('-')[1] if '-' in x else x)

    # Convert to numeric with error handling
    df[column_name + "_Min"] = pd.to_numeric(df[column_name + "_Min"], errors='coerce')
    df[column_name + "_Max"] = pd.to_numeric(df[column_name + "_Max"], errors='coerce')

      
    df[column_name + "_" + "Average"] = (df[column_name + "_" + "Min"] +  df[column_name + "_" + "Max"]) / 2
          
    df[column_name + "_" + "Average"] = df[column_name + "_" + "Average"].round(decimals = 0)

    return df


### define the function for Revenue cleansing column 

def Revenue_Cleansing_Column(df, column_name): 
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    # Replace 'to' with '-'
    df[column_name + "_Adjusted"] = df[column_name].str.replace('to', '-')
    df[column_name + "_Adjusted"] = df[column_name + "_Adjusted"].str.replace('(', '-')

    # Splitting into two parts
    df["first_part"] = df[column_name + "_Adjusted"].apply(lambda x: x.split('-')[0] if '-' in x else x)
    df["second_part"] = df[column_name + "_Adjusted"].apply(lambda x: x.split('-')[1] if '-' in x else x)
    df["first_part"] = df["first_part"].replace('\$', "", regex=True)
    df["Second_part_Adjusted"] =  df["second_part"].replace(['million', 'billion','\$'], "", regex=True)

    # Convert strings to numeric values
    df["first_part"] = pd.to_numeric(df["first_part"], errors='coerce')
    df["Second_part_Adjusted"] = pd.to_numeric(df["Second_part_Adjusted"], errors='coerce')
    # Handling million and billion
    mask_million = df["second_part"].str.contains("million", case=False)
    mask_billion = df["second_part"].str.contains("billion", case=False)

    df[column_name + "_Min"] = np.where(mask_million, df["first_part"] * 1e6, df["first_part"])
    df[column_name + "_Min"] = np.where(mask_billion, df["first_part"] * 1e9, df["first_part"])
    
    df[column_name + "_Max"] = np.where(mask_million, df["Second_part_Adjusted"] * 1e6, df["Second_part_Adjusted"])
    df[column_name + "_Max"] = np.where(mask_billion, df["Second_part_Adjusted"] * 1e9, df["Second_part_Adjusted"])

    # Calculating average
    df[column_name + "_Average"] = (df[column_name + "_Min"] + df[column_name + "_Max"]) / 2

    # Rounding to the nearest whole number
    df[column_name + "_Average"] = df[column_name + "_Average"].round(decimals=0)

    return df





### Some Data Engineering methods 

### Define the function for Rating Discretization function  


def Rating_Discretized(df, column_name):
    '''
    Passing data frame and the column which needs to be discretized 
    '''
    # Convert the column to numeric
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    Column_Group = []  # define array structure
    for rate in df[column_name]:
        if 0 < rate <= 1:
            Column_Group.append("1")
        elif 1 < rate <= 2:
            Column_Group.append("2")
        elif 2 < rate <= 3:
            Column_Group.append("3")
        elif 3 < rate <= 4:
            Column_Group.append("4")
        elif 4 < rate <= 5:
            Column_Group.append("5")
        else:
            Column_Group.append(None)

    # Add the discretized column to the DataFrame
    df[f"{column_name}_Discretized"] = Column_Group
    return df

### Defining hiring poistion discretization function 

def Hiring_Position(df, column_name): 
    '''
    Takes data frame and the job title name 
    '''
                
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    Column_Group = []
    
    for title in df[column_name]:
        title_lower = title.lower()

        if "data scientist" in title_lower and ("sr" in title_lower or "senior" in title_lower):
            Column_Group.append("Senior Data Scientist")
        elif "data scientist" in title_lower:
            Column_Group.append("Data Scientist")
        elif "data" in title_lower and "science" in title_lower:
            Column_Group.append("Data Scientist")
        elif "data engineer" in title_lower:
            Column_Group.append("Data Engineer")
        elif "data" in title_lower and "engineer" in title_lower:
            Column_Group.append("Data Engineer")
        elif "computer" in title_lower and "scientist" in title_lower:
            Column_Group.append("Data Scientist")
        elif "architect" in title_lower :
            Column_Group.append("Data Arhitect")
        elif "data analyst" in title_lower:
            Column_Group.append("Data Analyst")
        elif "data modeler" in title_lower:
            Column_Group.append("Data Modeler")
        elif "analyst" in title_lower:
            Column_Group.append("Analyst")
        elif "machine learning" in title_lower and ("sr" in title_lower or "senior" in title_lower):
            Column_Group.append("Senior Machine Learning Engineer")
        elif "machine" in title_lower and "learning" in title_lower:
            Column_Group.append("Machine Learning Engineer")
        elif "director" in title_lower:
            Column_Group.append("Director")
        elif "manager" in title_lower:
            Column_Group.append("Manager")
        elif "research" in title_lower:
            Column_Group.append("Resreacher")
        elif "scientist" in title_lower and "senior" in title_lower:
            Column_Group.append("Senior Scientist")
        elif "vice" in title_lower:
            Column_Group.append("Vice President")
        elif "scientist" in title_lower:
            Column_Group.append("Scientist")
        else:
            Column_Group.append(None)
        
    df[column_name + "_Adjusted"] = Column_Group
    
    return df

### Define Extract tools function 
def Extract_tools(df, column_name, list_name):
    """
    Extract keywords from the specified column in the DataFrame.

    Parameters:
    - df: DataFrame
    - column_name: str, the column containing job descriptions 
    - list_name: list of str, keywords to extract

    Returns:
    - DataFrame with additional columns for each keyword
    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")


    # Convert the specified column to lowercase and remove non-alphanumeric characters
    df[column_name] = df[column_name].astype(str).str.lower().replace('[^\w\s]', '', regex=True)

    # Use list comprehension to create binary columns for each keyword
    for word in list_name:
        df[word] = df[column_name].str.contains(r'\b' + re.escape(word) + r'\b', regex=True).astype(int)

    return df

### Define Extract company age 
def Extract_company_age(df,column_name): 
    '''
    The user passes the data frame to function aligned with the column name 
    
    '''
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")


    ### Compute the current data year
    current_year = datetime.now().year
       # Convert the column to numeric, handling errors with 'coerce'
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    
    # Calculate the company age
    df['Company_Age'] = current_year - df[column_name].replace(-1, current_year)
    
    # Replace negative or invalid values with -1
    df['Company_Age'] = df['Company_Age'].apply(lambda x: -1 if x < 1 else x)
    
    return df

### Define the extract state function 

def Extract_state(df, column_name): 
    
    df[column_name] = df[column_name].str.extract('(\D{2})')
    df[column_name]=df[column_name].str.upper()
    df
    
    return df


### Define the function for impute columns 

def impute_columns(df, columns, method, knn_neighbors=None, features_for_regression=None):
    for column_name in columns:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

        if method == 'mean':
            df[column_name].fillna(df[column_name].mean(), inplace=True)
        elif method == 'median':
            df[column_name].fillna(df[column_name].median(), inplace=True)
        elif method == 'mode':
            df[column_name].fillna(df[column_name].mode().iloc[0], inplace=True)
        elif method == 'knn':
            knn_imputer = KNNImputer(n_neighbors=knn_neighbors)
            df[column_name] = knn_imputer.fit_transform(df[[column_name]])
        elif method == 'regression' and features_for_regression:
            regression_df = df[features_for_regression + [column_name]].dropna()
            X = regression_df[features_for_regression]
            y = regression_df[column_name]
            regression_model = LinearRegression()
            regression_model.fit(X, y)
            df.loc[df[column_name].isna(), column_name] = regression_model.predict(df.loc[df[column_name].isna(), features_for_regression])
        else:
            raise ValueError("Invalid imputation method or missing required parameters.")

    return df

def imputation_task(**kwargs):
    # Load your data here
    data_path = '/path/to/your/data.csv'
    df = pd.read_csv(data_path)

    # Specify the columns to impute
    columns_to_impute = ['Salary_Estimate_Average', 'Salary_Estimate_Min', 'Salary_Estimate_Max', 'Size_Min',
    'Size_Max', 'Size_Average', 'Revenue_Min','Revenue_Max', 'Revenue_Average']

    # Perform imputation (example using mean)
    imputed_df = impute_columns(df, columns_to_impute, method='median')

    return df

### Define the function for replacing outlier with median 

def replace_outliers_with_median(df, columns_list):
    replaced_df = df.copy()

    for column in columns_list:
        # Calculate the IQR for the column
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1

        # Define the upper and lower bounds for outliers
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Identify outliers
        outliers_mask = (df[column] < lower_bound) | (df[column] > upper_bound)

        # Replace outliers with the median of the column
        replaced_df.loc[outliers_mask, column] = df[column].median()

    return replaced_df


def outlier_replacement_task(**kwargs):
    # Load your data here
    data_path = '/path/to/your/data.csv'
    df = pd.read_csv(data_path)

    # Specify the columns to replace outliers
    columns_to_replace_outliers = ["Salary_Estimate_Min","Salary_Estimate_Max","Salary_Estimate_Average", 
                                     "Size_Min",
                                     "Size_Max", 
                                     "Size_Average",
                                     "Revenue_Min", 
                                     "Revenue_Max", 
                                     "Revenue_Average"]

    # Replace outliers with median
    replaced_df = replace_outliers_with_median(df, columns_to_replace_outliers)

    return df

### Define the function for encoding 

def number_encode_features(df): 
    '''
    Passing a data frame then copying the inital data 
    '''
    for column in df.columns:
        if df.dtypes[column] == np.object: ## detecting whether the attribute is type of object

            df[column + "_encoded"] = preprocessing.LabelEncoder().fit_transform(df[column]) ## applying label enconding 
    return df

### define function for scaling 


def scaling_data(df, columns_list, method): 
    
    '''
    Passing data frame aligned with the method to normalize numeric columns as each column condition best fits 
    '''
    
    if method == "Min_Max": ## Min max scaling 
        for column in columns_list:
            df[column + "_Scaled"] = MinMaxScaler().fit_transform(df[[column]])
        
    elif method == "Standard": ### standardized scaling
        for column in columns_list:
            df[column + "_Scaled"] = StandardScaler().fit_transform(df[[column]])
    elif method == "Robust": ## robust scaling in case there was outliers that would pull scaling towards them 
        for column in columns_list: 
            df[column + "_Scaled"] = RobustScaler().fit_transform(df[[column]])
        
    elif method == "Max_ABS": ## maximum absolute scaling 
        for column in columns_list: 
            df[column + "_Scaled"] = MaxAbsScaler().fit_transform(df[[column]])
    else:
        raise ValueError("Invalid scaling method or missing required parameters.")


    return df

### define function for normalization 

def normalizing_data(df, columns_list, method): 
    
    '''
    Passing data frame aligned with the method to normalize numeric columns as each column condition best fits 
    '''
    
    from sklearn.preprocessing import PowerTransformer
    
    ### If the column doesnot have outliers, normalzition can be used
    if method == "L1":  ### letting the sum of the absolute values of each row equal 1 
        for column in columns_list:
            df[column + "_Normalied"] = Normalizer(norm='l1').fit_transform(df[[column]])
        
    elif method == "L2": ### letting the sum of squared values of each row equal 1
        for column in columns_list:
            df[column + "_Normalized"] = Normalizer(norm='l2').fit_transform(df[[column]])
    elif method == "PowerTransformer": 
        for column in columns_list: ## Letting the data looks more gausains
            df[column + "_Normalized"] = PowerTransformer(method='yeo-johnson').fit_transform(df[[column]])
        
    else:
        raise ValueError("Invalid scaling method or missing required parameters.")


    return df




### define the function for dropping non needed columns after data engineering

def dropping_after_transform(df, columns_list): 
    ''' takes data frame and the column list which need to be dropped
    
    '''
    
    new_df = df.drop(columns=columns_list, axis=1, inplace= True)
    
    return new_df 


### define the function for exporting cleaned data 

def export_cleaned_data(df): 
    
    df.to_csv('cleaned_data.csv', index=False)

########################################################################




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 25),
    'retries': 1


}

# Instantiate the DAG

Milestone = DAG(
    'test',  # Should match the DAG filename without the .py extension
    default_args=default_args,
    description='A DAG to load data from CSV, rename columns, and remove duplicates...',

)

# Define the path to your CSV file
data_path = 'Uncleaned_DS_jobs.csv'





########################################################################


# Define the load data task
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=loading_Data,
    op_args=[data_path],
    dag=Milestone,
)

# Define the rename columns task
rename_columns_task = PythonOperator( ## type of action operator python operator 
    task_id='rename_columns_task',
    python_callable=Rename_columns,
    op_args=[load_data_task.output],
    provide_context=True,
    dag=Milestone,
)


# Define the remove duplicates task
remove_duplicates_task = PythonOperator( ## type of action operator python operator 
    task_id='remove_duplicates_task',
    python_callable=Removing_duplicates,
    op_args=[load_data_task.output],
    provide_context=True,
    dag=Milestone,
)

## Define the operator task for tidying the columns 
tidy_columns_task = PythonOperator( ## type of action operator 
    task_id='tidy_columns_task',
    python_callable=Tidying_columns,
    op_args=[remove_duplicates_task.output],
    provide_context=True,  
    dag=Milestone,
)

## Define the function for dropping non needed columns 

drop_columns_task = PythonOperator( ## type of an action operator 
    task_id='drop_columns_task',
    python_callable=dropping_non_needed_columns,
    op_args=[tidy_columns_task.output, ['Competitors', 'index']],  # dropping both  competitors and index 
    provide_context=True,
    dag=Milestone,
)

## Define the operator that will remove character from text 
remove_numbers_text_task = PythonOperator( ## using the type of the python operator 
    task_id='remove_numbers_text_task',
    python_callable=Removing_numbers_f_text,
    op_args=[drop_columns_task.output, 'Company_Name'], ## passing the company name column 
    provide_context=True,
    dag=Milestone,
)

special_characters_task = PythonOperator(
    task_id='special_characters_task',
    python_callable=Special_characters_removed,
    op_args=[drop_columns_task.output, 'Salary_Estimate'],  ## passing the salary estimate column
    provide_context=True,
    dag=Milestone,
)


size_characters_task = PythonOperator(
    task_id='size_characters_task',
    python_callable=Size_characters_removed,
    op_args=[drop_columns_task.output, 'Size'],  ## passing the size column
    provide_context=True,
    dag=Milestone,
)

revenue_cleansing_task = PythonOperator(
    task_id='revenue_cleansing_task',
    python_callable=Revenue_Cleansing_Column,
    op_args=[drop_columns_task.output, 'Revenue'],  ## passing the revenue column 
    provide_context=True,
    dag=Milestone,
)



#################################################################################################

### we need to define here the combined outout funtion and its operator 


## since the function of Removing_number_f_text, Sepcial_characters_removed, Size_characters_removed and the Revenues_cleaning will run in parallel, 
## a new combined function will be dfine to combine the output of the pre-defined function into one function 

def combine_outputs_function(**kwargs):
    ti = kwargs['ti']

    # Retrieve the outputs from the task instances
    remove_numbers_text_output = ti.xcom_pull(task_ids='remove_numbers_text_task')
    special_characters_output = ti.xcom_pull(task_ids='special_characters_task')
    size_characters_output = ti.xcom_pull(task_ids='size_characters_task')
    revenue_cleansing_output = ti.xcom_pull(task_ids='revenue_cleansing_task')

    # Combine the outputs into a single DataFrame
    combined_df = pd.concat([remove_numbers_text_output, special_characters_output, size_characters_output, revenue_cleansing_output], ignore_index=True)

    # Identify common columns across all DataFrames
    common_columns = set.intersection(*(set(df.columns) for df in [remove_numbers_text_output, special_characters_output, size_characters_output, revenue_cleansing_output]))

    # Drop common columns from the combined DataFrame
    combined_df = combined_df.drop(columns=common_columns, errors='ignore')

    # Remove duplicates from the combined DataFrame
    combined_df = combined_df.drop_duplicates()

    return combined_df

combine_outputs_task = PythonOperator(
    task_id='combine_outputs_task',
    python_callable=combine_outputs_function,  # calling the cobmine output function defined before 
    op_args=[remove_numbers_text_task.output, special_characters_task.output, size_characters_task.output, revenue_cleansing_task.output],
    provide_context=True,
    dag=Milestone,
)


#################################################################################################


discretize_rating_task = PythonOperator(
    task_id='discretize_rating_task',
    python_callable=Rating_Discretized,
    op_args=[combine_outputs_task.output, 'Rating'],  ## passing the Rating column
    provide_context=True,
    dag=Milestone,
)

hiring_position_task = PythonOperator(
    task_id='hiring_position_task',
    python_callable=Hiring_Position,
    op_args=[combine_outputs_task.output, 'Job_Title'],  ## passing the job title column 
    provide_context=True,
    dag=Milestone,
)

extract_tools_task = PythonOperator(
    task_id='extract_tools_task',
    python_callable=Extract_tools,
    op_args=[combine_outputs_task.output, 'Job_Description', ['python', 'excel', 'hadoop', 'spark', 'aws', 'tableau', 'big_data']],  # Replace 'job_description_column' and the list of tools with your actual column name and tools
    provide_context=True,
    dag=Milestone,
)

extract_company_age_task = PythonOperator(
    task_id='extract_company_age_task',
    python_callable=Extract_company_age,
    op_args=[combine_outputs_task.output, 'Founded'],  ## passing the founded column
    provide_context=True,
    dag=Milestone,
)

extract_state_task = PythonOperator(
    task_id='extract_state_task',
    python_callable=Extract_state,
    op_args=[combine_outputs_task.output, 'Location'],  ## passing the location column 
    provide_context=True,
    dag=Milestone,
)

####################################################################################

### defining the output function that will cobine all the feature engineering tasks 


### define the function that combine the output of the discretize rating, hiring poisition , 
## extract tools, extract company age and extract state 

def combine_Feature_Engineering_function(**kwargs):
    ti = kwargs['ti']

    discretize_rating_output = ti.xcom_pull(task_ids='discretize_rating_task')
    hiring_position_output = ti.xcom_pull(task_ids='hiring_position_task')
    extract_tools_output = ti.xcom_pull(task_ids='extract_tools_task')
    extract_company_age_output = ti.xcom_pull(task_ids='extract_company_age_task')
    extract_state_output = ti.xcom_pull(task_ids='extract_state_task')

    # Combine the outputs into a single DataFrame
    combined_df = pd.concat([discretize_rating_output,hiring_position_output, extract_tools_output,  extract_company_age_output, extract_state_output], ignore_index=True)

    # Identify common columns across all DataFrames
    common_columns = set.intersection(*(set(df.columns) for df in [discretize_rating_output,hiring_position_output, extract_tools_output, extract_company_age_output, extract_state_output]))

    # Drop common columns from the combined DataFrame
    combined_df = combined_df.drop(columns=common_columns, errors='ignore')

    # Remove duplicates from the combined DataFrame
    combined_df = combined_df.drop_duplicates()

    return combined_df



combine_feature_engineering_task = PythonOperator(
    task_id='combine_feature_engineering_task',
    python_callable=combine_Feature_Engineering_function,
    provide_context=True,
    dag=Milestone,
)

impute_column_task =  PythonOperator(
    task_id='imputation_task',
    python_callable=imputation_task,
    provide_context=True, 
    op_args=[combine_feature_engineering_task.output],  
    dag=Milestone,
)


outlier_replacement = PythonOperator(
    task_id='outlier_replacement_task',
    python_callable=outlier_replacement_task,
    provide_context=True,  
    op_args=[impute_column_task.output],  
    dag=Milestone,
)



number_encode_task = PythonOperator(
    task_id='number_encode_task',
    python_callable=number_encode_features,
    op_args=[outlier_replacement.output],  
    provide_context=True,
    dag=Milestone,
)

scaling_data_task = PythonOperator(
    task_id='scaling_data_task',
    python_callable=scaling_data,
    op_args=[number_encode_task.output, ['Rating', 'Salary_Estimate_Min', 'Salary_Estimate_Max'], "Robust"],  
    provide_context=True,
    dag=Milestone,
)

normalizing_data_task = PythonOperator(
    task_id='normalizing_data_task',
    python_callable=normalizing_data,
    op_args=[number_encode_task.output, ["Founded", "Size_Min", "Size_Max", "Revenue_Min", "Revenue_Max", "Company_Age"], "PowerTransformer"],  
    provide_context=True,
    dag=Milestone,
)

def combine_scaling_normalizing_outputs(**kwargs):
    ti = kwargs['ti']

    scaling_output= ti.xcom_pull(task_ids='scaling_data_task')
    normalizing_output = ti.xcom_pull(task_ids='normalizing_data_task')
 

    # Combine the outputs into a single DataFrame
    combined_df = pd.concat([scaling_output,normalizing_output], ignore_index=True)

    # Identify common columns across all DataFrames
    common_columns = set.intersection(*(set(df.columns) for df in [scaling_output,normalizing_output]))

    # Drop common columns from the combined DataFrame
    combined_df = combined_df.drop(columns=common_columns, errors='ignore')

    # Remove duplicates from the combined DataFrame
    combined_df = combined_df.drop_duplicates()

    return combined_df

combine_scaling_normalizing_task = PythonOperator(
    task_id='combine_scaling_normalizing_task',
    python_callable=combine_scaling_normalizing_outputs,
    provide_context=True,
    dag=Milestone,
)


drop_after_transform_task = PythonOperator(
    task_id='drop_after_transform_task',
    python_callable=dropping_after_transform,
    op_args=[combine_scaling_normalizing_task.output, ['Job_Title', 'Salary_Estimate', 'Job_Description', 'Rating',
       'Company_Name', 'Location', 'Headquarters', 'Size', 'Founded',
       'Type_of_ownership', 'Industry', 'Sector', 'Revenue', 'first_part',
       'second_part', 'Salary_Estimate_Adjusted', 'Salary_Estimate_Min',
       'Salary_Estimate_Max', 'Salary_Estimate_Average', 'Size_Adjusted',
       'Size_Min', 'Size_Max', 'Size_Average', 'Revenue_Adjusted',
       'Second_part_Adjusted', 'Revenue_Min', 'Revenue_Max', 'Revenue_Average',
       'Rating_Discretized', 'Job_Title_Adjusted']],  
    provide_context=True,
    dag=Milestone,
)


export_cleaned_data_task = PythonOperator(
    task_id='export_cleaned_data_task',
    python_callable=export_cleaned_data,
    op_args=[drop_after_transform_task.output],  
    provide_context=True,
    dag=Milestone,
)

##############################################################################

## defining the dependencies

# Set the task dependencies
## first the data will be loading by giving the csv path and then the data will be renamed and removing 
## the duplicates concurrently and the output will be feed into the tidy_columns_tak 
## after all the primary data cleansing is done , once can get into dropping non needed columns

load_data_task >> [rename_columns_task, remove_duplicates_task] >> tidy_columns_task >> drop_columns_task >> [remove_numbers_text_task, special_characters_task,size_characters_task,revenue_cleansing_task] >> combine_outputs_task >> [discretize_rating_task,hiring_position_task,
extract_tools_task,extract_company_age_task,extract_state_task] >> combine_feature_engineering_task  >> impute_column_task  >> outlier_replacement >> number_encode_task >> [scaling_data_task,normalizing_data_task] >> combine_scaling_normalizing_task>>  drop_after_transform_task >> export_cleaned_data_task



if __name__ == "__main__":
    Milestone.cli()