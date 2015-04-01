# Kojak

## Where do people with Ph.D.\'s in physics end up working?

### Project Design and To-Do

1. Check what information I can get from Linkedin, specifically the following:
    * [x] degree. In the Education Field.
    * [x] which university
    * [x] university location
    * [x] year
    * [x] Current position. In the Company Field.
    * [x]  (Past positions?)
    * [x] Job location. In the Location Field
1. Build the API and get a sample of data.
2. While retrieving the other data, start building a model:
    * Categorize by hand some jobs by industry
    * Make a model automatically assign industry for you
    * **[EDIT]** there is already an industry code for you; you can probably skip this step.

### Deliverables
I want to build three main deliverables:

1. A model that predicts what jobs you will likely be doing after your graduation.

1. a simple bar chart where the categorical variable (X) is the industry (Finance, Tech, Data Science, Academia, etc.) and Y is the corresponding fraction of people.
2. a heat map of the US with hot spots in the centers where the jobs are located.

3. (*optional*) a heat map of the US with hot spots in the universities' locations.
4. (*very optional*) a network of connections between job industry and specific universities.

### Leads to look after
* In the [Company Fields](https://developer.linkedin.com/docs/fields/companies) there is something called [industry codes](https://developer.linkedin.com/docs/reference/industry-codes).


### Questions and Problems to solve:
1. Physics only, or high-energy physics?
2. What database to use? Relational or not?