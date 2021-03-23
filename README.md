# Commendaroo

### Release Notes

0.10.0:
- Now generic recommendations are created: mostPopularAll, mostPopularCatchUp, mostPopularKids, mostPopularFilm, mostPopularFilmPurchases, mostPopularFilmRentals, mostPopularSport, mostPopularTV
- The For You calculation is improved to run faster (up to 50% faster)

0.9.0:
- Now For you hides past items only for specific past time windows (90 days for scheduler channel Film/TV/Sport; 30 days for Kids/TV Replay)

0.8.1:
- Now try statements in get metadata functions avoid dropping some items without history because they are missing some features (this was an issue for new releases)
- From above, follows that items yet to be released now get their metadata from the product table
- Fixed bug that kept wrong order of MLT items without history

0.8.0:
- Now model creates More Like This also for items without history

0.7.2:
- Changed function to upload recommendations to dynamoDB tables, using the new API

0.7.1:
- Now music items are included
- Fixed bug where some items were skipped in More Like This accidentally

0.7.0:
- Now More Like This also uses sub-genres for similarity
- Now event strentgh is the same for all types of events (view/purchase/rental/ppv)
- Now More Like This only saves output in format type|guid
- Fixed missing synopsis by extending scanned tables

0.6.1:
- For You recommendations are now uploaded to dynamoDB in batches to try and avoid possible AWS limit

0.6.0:
- Now model doesn't load data of customers who have opted out of recommendations
- Model now scores whole base of customers for For You (excluding opt outs)

0.5.0:
- Now synopsis is also used for More Like This
- Fixed availability rule also using availability start date and status columns from EDW data
- Now dropping all episode/season items that do not have an active brand so they are not accidentally recommended

0.4.1:
- Fixed score in For You table for 'anonymous' to have a number rather than nan (popularity ranked from him to low)

0.4.0:
- Genre now matches genre of the seed in More Like This
- Scheduler channel now matches scheduler channel of the seed in More Like This
- For scheduler channel Kids rating now matches only rating of seed
- Season-to-brand is wrapped inside try/except to move forwarm if the on-prem queries fail

0.3.4:
- Added an 'anonymous' entry to the For You table storing some simple most popular recommendations to be tweaked in the future

0.3.3:
- More Like This now also offers the ```<type>|<guid>``` look-up option in the dynamo table
- The Athena tables created/dropped have a dev suffix to avoid conflicting with scoring table

0.3.2:
- More Like This recommends only content with rating up to 1 degree higher than the seed

0.3.1:
- Updated the format of the of output json
- Fixed bug on purchase/rental/ppv table creation that resulted in missing films (views were not affected)

0.3.0:
- Chtistmas 2020 end-to-end working tech trial 
