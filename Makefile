
OAUTH_CREDENTIALS = $(shell cat test-credentials.txt)
export OAUTH_CREDENTIALS

JARS = $(shell ls build/deps/*.jar)
noop=
space = $(noop) $(noop)

CPJARS = $(subst $(space),:,$(JARS))

run: test-credentials.txt
	./gradlew runMod -i
	
test clean install assemble:
	./gradlew $@
	
test-credentials.txt:
	@echo "To test this you must create $@; see the readme."
	@exit 1
	
jython:
	CLASSPATH=$(CPJARS) jython $(JARGS)