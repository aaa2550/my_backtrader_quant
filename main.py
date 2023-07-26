from ContinueRisingBot import ContinueRisingBot


def main():
    bot = ContinueRisingBot(max_positions=10, use_cache=True)
    bot.run()


if __name__ == '__main__':
    main()
