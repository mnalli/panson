FROM python:3.9-trixie

WORKDIR /home

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["jupyter", "notebook", "--allow-root", "--ip='*'", "--NotebookApp.token=''", "--NotebookApp.password=''"]
