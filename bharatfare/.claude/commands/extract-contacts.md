# Extract Contacts from URL

Extract business contact information (emails, phones, names) from a website.

## Instructions

Target: $ARGUMENTS

1. Use the `fetch` MCP tool to get the page content
2. Use the `playwright` MCP tool if the page needs JavaScript rendering
3. Extract all available contact data:
   - **Email addresses** (filter out generic ones like info@, noreply@, etc.)
   - **Phone numbers** (Indian format: +91 or 10-digit)
   - **Contact person names** and designations
   - **Company name**
   - **Physical address**, city, state, pincode
   - **GST numbers** (format: 2-digit state code + 5 alpha + 4 digit + 1 alpha + 1 digit + Z + 1 alphanumeric)
   - **Social media profiles**
   - **Website URLs**
4. If the page has multiple businesses listed, extract from all of them
5. Follow "Contact Us" or "About" links if found on the page
6. Present the data in a clean table format
7. Offer to save the results as a CSV file
