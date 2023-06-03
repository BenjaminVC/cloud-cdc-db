FROM public.ecr.aws/lambda/python:3.8

COPY . .

RUN pip install psycopg2-binary

# Command can be overwritten by providing a different command in the template directly
CMD ["lambda_function.lambda_handler"]
