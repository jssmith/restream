# Example Queries - In English #

In order to inform our language design we here collect a library of example queries drawn from several domains. Our hope, in presenting these queries in English, is to suggest an idiomatic and natural form of expression, one that our language might attempt to mimic.

## Best Buy product search ##

This example involves providing search results and product recommendation in an online shopping context. Queries are issued during document scoring, as well as during training. See the Antelope [documentation][bestbuy-doc] and [code][bestbuy-code].

### Events ###

* `ProductUpdate`
  - `timestamp` - time of event
  - `sku` - product selected
  - `name` - title / one-line description
  - `description` - paragraph-length description
* `ProductView`
  - `timestamp` - time of event, in this case when the user selected a search result
  - `queryTimestamp` - time at which the user issued the search query
  - `query` - text string entered in the query box
  - `userId` - user issuing the query
  - `skuSelected` - matches `sku` in `ProductUpdate` to indicate which product the user selected from among the search results.

### Queries ###

- **Overall popularity(`sku`)** - fraction of all product views that are for product `sku`.
- **Term popularity(`sku`,`queryStr`)** - product, by `term`∈`terms(queryStr)`, of fraction of searches containing `term` that result in selecting product `sku`.
- **Naive Bayes(`sku`,`query`)** - TermPopularity(`sku`,`query`) × OverallPopularity(`sku`)
- **Term frequency-inverse document frequency(`sku`,`queryStr`)** - sum over each `term`∈`terms(queryStr)` of: (number of occurrences of `term` in title of most recent product update of product identified by `sku`)^0.5 × (1 + log((number of documents)/(1 + number of documents that contain `term` in most recent update)))^2

## Dating recommendations ##

Some of the examples below are also documented in the Antelope [code][dating-code].

### Events ###

* `NewUserEvent`
  - `timestamp` - time of the event, in this case the time at which the user completes registration
  - `newUserId` - unique identifier for this user account
* `VoteEvent`
  - `timestamp` - time of the event, in this case time at which the user votes
  - `voterUserId` - id for the user voting
  - `votedUserId` - id for the user voted upon
  - `vote` - either `positive` or `negative`, indicating interest in the displayed user
* `MessageEvent` - metadata on a message (no content for this analysis)
  - `timestamp` - time of event
  - `senderUserId` - user who sends the message
  - `recipientUserId` - user who receives the message

### Queries ###

- **Same Region(`userIdA`,`userIdB`)** - is the last update of geographic region for `userIdA` the same as the last update of geographic region for `userIdB`?
- **Bordering Region(`userIdA`,`userIdB`)** - is the last update of geographic region for `userIdA` one that borders the last update of geographic region for `userIdB`?
- **User Activity(`userId`)** - how long ago was `userId` last active (login event, action event of any sort).
- **Age difference absolute value(`userIdA`, userIdB`)** - absolute value of difference between last updated age for `userIdA` and last updated age for `userIdB`.
- **Are ages in same bucket(`userIdA`,`userIdB`)** - is the age of `userIdA` in the same 5-range as the age of `userIdB` (i.e., ⌊`ageOf(userIdA)`/5⌋ = ⌊`ageOf(userIdB)`/5⌋)?
- **Positive vote fraction(`userId`)** - fraction of `VoteEvents` where `userId` is voting that result in a `positive` vote value. Compute for all time, recent window, exponentially smoothed with time constant κ.
- **Number of votes exceeds threshold(`userId`,`threshold`)** - is the number of votes made greater than the `threshold` value. Compute for all time, recent window, exponentially smoothed with time constant κ.
- **Recently active(`userId`,`period`)** - did the user vote within the interval `period` leading up to now?
- **Number of votes received since last voted(`userId`)** - how many `VoteEvent` where this user was voted on have occurred since last `VoteEvent` where this user voted? (strict count, limited time, or exponential smoothing)
- **Number outstanding votes(`userId`)** - for how many `VoteEvent`s is `userId` = `votedUserId` where there does not exist a following `VoteEvent` in which this `userId` = `votingUserId`?
- **Average time to answer a vote(`userId`)** - what is the average amount of time between a `VoteEvent` where `votedUserId`=`userId` and a corresponding response `voterId`=`userId` for `VoteEvent`s in the recent interval (exponential smoothing also possible)?
- **Fraction of votes received remaining outstanding(`userId`)** - of the `VoteEvent` where `votedUserId`=`userId` during the past window (fixed time period or exponentially smoothed), what fraction have a corresponding `VoteEvent` where `voterUserId`=`userId`?
- **Model match probability for recent votes(`userId`,`model`)** - of the `VoteEvent`s in the past window (fixed time period or exponentially smoothed) where `voterUserId`=`userId` and where there does not exist a preceding `VoteEvent` to which this is a response, what is the average and what is the minimum probability of a match evaluated by `model`?


## Social network anti-spam ##

### Events ###

* `NewUserEvent`
  - `timestamp` - time of the event, in this case the time at which the user completes registration
  - `newUserId` - unique identifier for this user account
  - `gender` - is this account registered as `female` or `male`
  - `ipAddress` - registration ip address
* `LoginEvent`
  - `timestamp` - time of event
  - `loginUserId` - unique identifier for this account
  - `ipAddress` - login ip address
* `MessageEvent`
  - `timestamp` - time of event
  - `messageId` - unique identifier of the message
  - `senderUserId` - user who sends the message
  - `recipientUserId` - user who receives the message
  - `content` - text of message
* `NewFriendshipEvent`
  - `timestamp` - time of event
  - `userIdA` - one of two users who become friends
  - `userIdB` - the other of two users who become friends
* `ReportSpamEvent`
  - `timestamp` - time of event
  - `messageId` - identifier of the message flagged as abuse

### Queries ###

- **Fraction sends in are responses(`userId`)** - what fraction of messages sent by `userId` in the past 7 days were sent in response to a message received in the past 14 days?
- **Response rate to sends(`userId`)**: what fraction of messages sent by `userId` in between 24 hours ago and 7 days ago received a response within 24 hours?
- **Response rate to sends(`userId`)**: what fraction of messages sent by `userId` in the past 7 days have by now received a response?
- **Ramp up rate of message sends(`userId`)** - what fraction of messages sent by `userId` during the past 7 days were sent in the past 24 hours?
- **fraction of conversations initiated(`userId`)** - what fraction of conversations in which `userId` sent messages during the past 7 days are conversations that were initiated by `userId`. We define a conversation as an exchange of messages between two users. We define a user to have initiated a conversation with the user sends the first message in an exchange, and when there have been no messages between the two users in the preceding 30 days.
- **E-mail addresses in messages sent(`userId`)** - what fraction of messages sent by `userId` in the past 30 days have contained patterns matching an e-mail address regular expression?
- **E-mail addresses previously reported as spam sent(`userId`)** - what fraction of messages sent by `userId` in the past 30 days contained patterns matching an e-mail address that referenced in a previous `ReportSpamEvent`?
- **Messages to friends(`userId`)** - what fraction of messages sent by `userId` in the past 7 days were sent along an existing friend edge (`NewFriendshipEvent` connecting `userId` with the message recipient)?
- **New ip address(`userId`)** - is this user logging in from an ip address which has not previously been seen on this account?
- **Sudden increase in messages(`MessageEvent`)** - is this message message sent from an ip address that is experiencing a sudden increase in volume: is the number of messages sent during the past 24 hours is more than half of the number of messages sent during the past 7 days?
- *Can add many more…*

## Wikipedia anti-spam ##

- **Similarity** - is the cosine similarity between this revision and some revision of a previous document greater that 0.9?
- *Lots more to add here…*


[bestbuy-doc]: https://ifwe.github.io/antelope/doc/demo-best-buy.html
[bestbuy-code]: https://github.com/ifwe/antelope/blob/master/demo-best-buy/src/main/scala/co/ifwe/antelope/bestbuy/model/DemoBestBuyModel.scala
[dating-code]: https://github.com/ifwe/antelope/blob/master/demo-dating-simulation/src/main/scala/co/ifwe/antelope/datingdemo/model/DatingModel.scala