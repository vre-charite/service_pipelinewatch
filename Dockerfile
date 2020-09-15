FROM 10.32.42.225:5000/python:3.7-buster
RUN groupadd --gid 1004 deploy \
    && useradd --home-dir /home/deploy --create-home --uid 1004 \
        --gid 1004 --shell /bin/sh --skel /dev/null deploy
WORKDIR /home/deploy
COPY .  ./
RUN chown -R deploy:deploy /home/deploy
USER deploy
RUN chmod +x /home/deploy/gunicorn_starter.sh
RUN pip install --no-cache-dir -r requirements.txt --user --proxy="http://proxy.charite.de:8080/"
ENV PATH="/home/deploy/.local/bin:${PATH}"
CMD ["./gunicorn_starter.sh"]
