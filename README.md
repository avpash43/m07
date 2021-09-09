# Data Mart Vusualisation:

###### Plan damps stored in 'plan-dumps' folder.
###### We can look at SparkUI query plan for analyse 'bottlenecks'.
###### As you can see 'BroadcastExchange' before getting joined tables and 'Sort' operation was expensive.

![Alt text](images/Databricks-details-for-query.png?raw=true "Title")

###### Result files stored in ADLS:

![Alt text](images/target_files.jpg?raw=true "Title")

Top 10 hotels with max absolute temperature difference by month:

![Alt text](images/max_avg_tmpr_c_for_bookings.jpg?raw=true "Title")

Top 10 busy (e.g., with the biggest visits count) hotels for each month:

![Alt text](images/top_10_busy_hotels_for_each_month_gold.jpg?raw=true "Title")

Wweather trend:

![Alt text](images/top_10_hotels_with_max_abs_temp_diff_gold.jpg?raw=true "Title")


* Add your code in `src/main/` if needed
* Test your code with `src/tests/` if needed
* Modify notebooks for your needs
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch notebooks on Databricks cluster
