

### Issues:
- Many functions and query use begin_date and end_date. Everywhere the default is datetime.... This should be shorter.  
  Options:  
         
      replace f(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)) with 
      - f(start_date=START_DATE, ...):
      - f(start_date=None, ...):
            if not start_date: start_date=datetime(...)
      - f(..., **kwargs):
      - f(start_date=str2dt('2020-01-01')

- Investigate: when a tweet is deleted, twint finds it, but returns a profile iso tweet. 
    - Or is this a multiprocess issue? I don't have the porblem when only scraping tweets without profiles, or only profiles without tweets !!!
      Is it different when using multiprocess iso multi-threatning?    
    - The program throws exception  KeyError('user_id') at:   
     `twitter_facade.py / def save_tweets(tweets_df, update=True): / def _format_tweets_df(df):`
        
  Example bad tweet:
  
         tweet_id          name    username                                                bio                      url        join_datetime    join_date join_time  tweets location  following  followers  likes  media  private  verified                                             avatar background_image   
      0  2187299366  Vera Celis ✌  Vera_Celis  Burgemeester Stad Geel, mama van Sarah, Toon e...  http://www.veracelis.be  10 Nov 2013 2:52 PM  10 Nov 2013   2:52 PM     392     Geel        503       1448    241     63        0         0  https://pbs.twimg.com/profile_images/994218135...             None

  or: 
   
         tweet_id               name      username                                                bio                      url        join_datetime    join_date join_time  tweets          location  following  followers  likes  media  private  verified                                             avatar                                   background_image
      0  1053868090742792193  Axel Ronse  axel_ronse  gaat voor #Kortrijk culturele hoofdstad 2030 e...  http://www.axelronse.be  20 Oct 2018 9:38 PM  20 Oct 2018   9:38 PM    2183  Kortrijk, België       2348       1471   3095    224        0         0  https://pbs.twimg.com/profile_images/128207284...  https://pbs.twimg.com/profile_banners/10538680...
  Example good tweet:   
  
         tweet_id      conversation_id     created_at                 date timezone place                                              tweet hashtags cashtags     user_id user_id_str  username         name  day hour                                               link  retweet  nlikes  nreplies  nretweets quote_url search near geo source user_rt_id user_rt retweet_id                                           reply_to retweet_date translate trans_src trans_dest
      0  1212446140563644417  1212446140563644417  1577904660000  2020-01-01 19:51:00    +0200        Crapuul https://www.hln.be/brussel/zo-ging-het...       []       []  2438136788  2438136788  Zu_Demir  Zuhal Demir    3   19  https://twitter.com/Zu_Demir/status/1212446140...    False     472        82         55             None                                                [{'user_id': '2438136788', 'username': 'Zu_Dem...                                            
      0    2438136788



- Some data change after beeing scraped. Ex: nlikes, nretweets, etc. When rescraping it's not updated because duplicate!  
    Solved it with upsert tweets
    
        
    
    
   



### Inspiration
#### Configuration
- https://github.com/antcer1213/cervmongo/blob/master/cervmongo/config.py
