
OAUTH_CREDENTIALS = $(shell cat test-credentials.txt)
export OAUTH_CREDENTIALS

run: test-credentials.txt
	./gradlew runMod -i
	
clean install assemble:
	./gradlew $@
	
test-credentials.txt:
	@echo "To test this you must create $@; see the readme."
	@exit 1