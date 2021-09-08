Data Mart Vusualisation
![Alt text](vasualisation/max_avg_tmpr_c_for_bookings.jpg?raw=true "Title")
![Alt text](vasualisation/top_10_busy_hotels_for_each_month_gold.jpg?raw=true "Title")
![Alt text](vasualisation/top_10_hotels_with_max_abs_temp_diff_gold.jpg?raw=true "Title")

Plan damps stored in 'plan-dumps' folder.

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
