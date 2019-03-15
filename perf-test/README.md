### Test run command

```
mvn test -Ddatabase.connectionString="jdbc:mysql://localhost:3306/demo" \
-Ddatabase.importQuery="SELECT * FROM my_table3 WHERE $CONDITIONS" \
-Ddatabase.driverName=mysql -Ddatabase.user=root -Ddatabase.password=mysql \
-Ddatabase.sinkTable=my_table2 \
-Ddatabase.pluginName=Database 
```