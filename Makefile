# Install docker and docker-compose
all:
	sudo apt-get update
	sudo apt-get install docker.io docker-compose-v2 -y
	sudo usermod -aG docker ${USER}
	echo "Docker and docker-compose installed"
	sudo systemctl start docker
	echo "Docker service started"
	docker compose up -d
	echo "Docker compose started"
	echo "Docker and docker-compose setup complete"

clean:
	docker compose down

remove:
	sudo apt-get remove docker.io docker-compose-v2 -y
	sudo apt-get autoremove -y
	sudo apt-get autoclean -y
	