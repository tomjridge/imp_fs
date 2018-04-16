all:
	$(MAKE) -C src   all
	$(MAKE) -C src   install
	$(MAKE) -C bin   all

clean:
	$(MAKE) -C src clean
	$(MAKE) -C bin clean
