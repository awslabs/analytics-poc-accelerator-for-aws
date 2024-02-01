# Analytics POC UI

A simple UI for the analytics POC. 

Variables from the stack are injected at deploy time. In order to add more variables, 
update `_data/myProject.js` with the variable name and environment variable. Then update
the `env` variable in `../../lib/ui/landing-page.ts` with the matching variable. 

## References

- For integrating tailwind: https://dev.to/psypher1/lets-learn-11ty-part-7-adding-tailwind-5cdh
- Integrating env variables: https://www.11ty.dev/docs/data-js/#example-exposing-environment-variables