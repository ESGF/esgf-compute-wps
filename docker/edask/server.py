if __name__ == '__main__':
    from edask.portal.app import EDASapp

    app = EDASapp('0.0.0.0', 5670, 5671)

    app.run()
