#%%
import requests
import xml.etree.ElementTree as ET
import time
import mysql.connector

# The SQL database that will store the extracted data.
mydb = mysql.connector.connect(
    host="",
    user="",
    password="",
    database=""
)
mycursor = mydb.cursor()

# The list of BGG usernames that will be used to access the collections.
users = open('users.txt', 'r')

# The offline backup file that will store each rating.
ratings = open('ratings.psv', 'a', encoding="utf-8")

# The cycle that will extract the ratings of a batch of 1000 users. 
for i in range(1000):

    # Read in a username and remove the new line character from the end of it.
    username = users.readline().replace('\n', '')

    # Construct the collection request URL.
    fullurl = 'https://www.boardgamegeek.com/xmlapi2/collection?username='
    fullurl += username + '&stats=1'

    # Send the request to BGG that will queue it instead of responding.
    request = requests.get(fullurl)

    # Wait some time to get the request out of the queue.
    time.sleep(3)

    # Send the same request the second time to receive a valid response.
    request = requests.get(fullurl)

    # Log the current username.
    print(str(i + 1) + '. ' + username)

    # Backup the username to make it easier to continue in case of errors.
    lastuser = open('lastuser.txt', 'w')
    lastuser.write(str(i + 1) + '. ' + username + '\n')
    lastuser.close()

    # Convert the response to XML.
    root = ET.fromstring(request.content)

    # Iterate through the values.
    for sitemap in root:
        children = list(sitemap)
        try:
            # Check if the current rating is valid.
            if (children[4] and children[4][0].get('value') != 'N/A'):

                # Get the name of the game.
                name = children[0].text

                # Get the user rating of the game.
                rating = children[4][0].get('value').replace('.', ',')

                # Log and backup the rating.
                row = name + '|' + rating
                print(row)
                ratings.write(row + '\n')

                # Upload the rating to the database.
                sql = "INSERT INTO userratings (name, rating) VALUES (%s, %s)"
                val = (name, rating)
                mycursor.execute(sql, val)
                mydb.commit()
        except:
            print('error')

# Close the backup file.
ratings.close()


# %%


# %%
