# Authentication

This is a quick walkthrough of the possible way to configure this project and make it run on your BigQuery project.

## Disclaimer

Please remember this project is just a POC.

Be assured that the original author has no ill intent, but be also warned that 
the original author declines all responsibility for any user suffering any loss, 
GCP billing cost increase, or prejudice caused by:
- Any feature, bug or error in the code
- Any malicious code introduced into this project by a third party using any mean

  (malicious fork, dependency injection, dependency confusion, etc.)

This documentation aims at informing the user as well as possible about the supported ways to connect this project
to the user's GCP account, and the risks associated with each method. To learn more about GCP authentication, 
please refer to the official GCP documentation: 
https://cloud.google.com/docs/authentication/best-practices-applications


## Method 1 : Use application default credentials

- **Documentation**: https://cloud.google.com/bigquery/docs/authentication/getting-started
- **Pros**: easy as pie
- **Cons**: not very safe
- **Advice**:
  - Only do this with code that you trust
  - Use this with a dummy GCP account that only have acces to a sandbox project.
  - Don't use this method in production

### Step 1. Install gcloud

Follow the instructions here: https://cloud.google.com/sdk/docs/install

### Step 2. Generate application-default login

_(This step is necessary if you run this locally, but can be skipped if you run 
this project directly from inside GCP where the application default are pre-configured)_

Run this command in a terminal:
```shell
gcloud auth application-default login
```

You can revoke the credentials at any time by running:
```shell
gcloud auth application-default revoke
```

### Step 3. Configure this project

#### Option A. Set the name of your project directly in `conf.py`

Update this line in the file `conf.py`
```python
GCP_PROJECT = "Name of your BigQuery project"
```

#### _OR_

#### Option B. Set it as a variable in your environment
```shell
export GCP_PROJECT="Name of your BigQuery project"
```

## Method 2 : Use a service account

- **Documentation**: https://cloud.google.com/bigquery/docs/authentication/service-account-file
- **Pros**: More secure
- **Cons**: A little more work involved
- **Advice**:
  - We recommend this method
  - Use a dedicated service account for this project
  - Only give it the minimal access necessary for your test

### Step 1. Create a service account

Go to your project's Service Account page: https://console.cloud.google.com/iam-admin/serviceaccounts

_(Please make sure you select the correct project.)_

#### Create a new service account
For example, you can call it `bigquery-frame-poc`.

#### Grant the following roles to the service account

You can grant it the following rights
- `BigQuery Job User` on the project you want.
- `BigQuery Data Viewer` on the project (or just on the specific datasets) that you want.

_(If you want to grant access to a specific dataset, this can be done after the 
service account is created, directly in the BigQuery console, by clicking 
the "Share Dataset" button on a Dataset's panel)_

### Step 2. Create and download a new json key for this service account

Once the service account is created, click on it, go to the "KEYS" tab,
and click on the "ADD KEY" button. You will automatically download 
a json Oauth2 file for this service account. Store it somewhere on your
computer. 

_(If you have forked this repo and stored the credentials inside, 
be careful not to commit it accidentally, use `.gitignore`)_ 

### Step 3. Configure this project to point to this file


#### Option A. Set the path to the credentials file directly in `conf.py`

Update this line in the file `conf.py`
```python
GCP_CREDENTIALS_PATH = "Path to your service account credentials json file"
```

#### _OR_

#### Option B. Set it as a variable in your environment
```shell
export GCP_CREDENTIALS_PATH="Path to your service account credentials json file"
```


## Method 3 : Do it your way

The constructor of the `BigQueryBuilder` class takes a `google.cloud.bigquery.Client`
as argument, allowing the users to instantiate the client in any other way they
might prefer.

