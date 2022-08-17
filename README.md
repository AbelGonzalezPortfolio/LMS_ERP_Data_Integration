# Canvas LMS and Jenzabar ERP Data Integration Package.

## Summary
LMS_ERP_Data_Integration is a package that syncs data one-way from the Jenzabar Enterprise resource planning system to Canvas Learning management system. Two higly popular data management softwares in higher education. At its core this package will create and remove courses, sections, enrollments, and users based on their status in the Jenzabar ERP.

## What problem does it solve?
Administratives offices use Jenzabar to create semester terms, sections, courses, student users, faculty users, and enrollments (the relations between the datasets). This data gets saved to a SQL database setup by the Jenzabar system on first installation. THis data needs to be synced with Canvas LMS in order for students and faculty to interact with their courses (e.x. Submit assignments, Grading, Collaboration, etc.). Previous to this packaghe the solution entailed downloading a report from canvas through their website. Upload the extracted report to the SQL server, compare the data with pre-done SQL actions, download the result, and reupload to canvas through the admin interface. This was a tedious process for an already shrinked IT department. THis package automates this process from Jenzabar to Canvas with zero human interaction needed.

## How does it solve it?
LMS_ERP_Data_Integration extracts the current term (semester and year) from theJenzabar system via a SQL server connection. Then uses this term to extract the current active data from Canvas via Canvas' API, which is setup with OAuth Keys. Once extracted it runs the pre-defined SQL scripts that compare the datasets. FInally through API it re-upload the data to Canvas, effectively adding any new sections, courses, users, or enrollments. 

## Todo:
- [ ] Easy switch between development and production environments.
- [ ] Add unit tests.