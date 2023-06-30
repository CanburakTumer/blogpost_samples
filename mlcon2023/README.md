## Notes on Iowa Liquor Sales

Total number of records  26,592,863

### Zip Code DQ Issues
Number of 712-2 zipcode is 7,940  
Number of distinct zipcodes with '.' 503, total # of them is 7,572,696

__Solution__  

Apply filter `zip_code != '712-2'` and conversion `cast(cast(case when ends_with(zip_code, '.0') then substr(zip_code, 1, 5) else zip_code end as integer) as string)`.


### Vendor Number DQ Issues
Number of distinct vendor_number with '.' 308, total # of them is 4,592,854

__Length Distribution__  

| length | count |
|:--- | ---:|
| null | 9 |
| 2 | 4707536 |	
| 3 | 17292464 |
| 4 | 976077 |	
| 5 | 3616777 |

__Solution__  
```
vendor_number is not null

cast(cast(case when ends_with(vendor_number, '.0') then substr(vendor_number, 1, instr(vendor_number, '.')-1) else vendor_number end as integer)
```

### Category DQ Issues
Number of distinct category with '.' 114, total # of them is 25,575,889

__Length Distribution__  

| length | count |
|:--- | ---:|
| null | 16974 |
| 7 | 1000000 |
| 8 | 6 |	
| 9 | 25575883 |

__Solution__  
```
category is not null
length(category) != 8
cast(cast(case when ends_with(category, '.0') then substr(category, 1, instr(category, '.')-1) else category end as 
```

### Clean data info

Number of rows remaining 26,485,417

## Notes on NYC Green Taxi Trips 2022
Number of rows 767,953

