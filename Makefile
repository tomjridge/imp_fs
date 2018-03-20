all:
	$(MAKE) -C src   all
	$(MAKE) -C src   install
	$(MAKE) -C bin   all
