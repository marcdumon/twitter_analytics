

### ToDo:
- Many functions and query use begin_date and end_date. Everywhere the default is datetime.... This should be shorter.  
  Options:  
         
      replace f(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)) with 
      - f(start_date=START_DATE, ...):
      - f(start_date=None, ...):
            if not start_date: start_date=datetime(...)
      - f(..., **kwargs):
- 
    
        
    
    
   



### Inspiration
#### Configuration
- https://github.com/antcer1213/cervmongo/blob/master/cervmongo/config.py
